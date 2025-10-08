#include <memory>
#include <string>
#include <unordered_set>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Settings.h>
#include <Disks/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/CompressionMethod.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeConfiguration.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Compaction.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/ObjectStorage/Utils.h>
#include <fmt/format.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Logger.h>
#include "Core/Defines.h"
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

#if USE_AVRO

namespace DB::Setting
{
    extern const SettingsUInt64 iceberg_compaction_large_file_size;
}

namespace DB::Iceberg
{

bool needToMoveFile(IcebergDataObjectInfoPtr object, ContextPtr context, ObjectStoragePtr object_storage)
{
    if (!object->position_deletes_objects.empty() || !object->equality_deletes_objects.empty())
        return false;
    auto object_size = object_storage->getObjectMetadata(object->relative_path).size_bytes;
    if (object_size > context->getSettingsRef()[Setting::iceberg_compaction_large_file_size])
        return false;
    return true;
}

using FileFilter = std::function<bool(IcebergDataObjectInfoPtr, ContextPtr, ObjectStoragePtr)>;

class FilterFileIterator : public IObjectIterator
{
public:
    FilterFileIterator(
        ObjectIterator iterator_,
        ObjectStoragePtr object_storage_,
        ContextPtr context_,
        FileFilter filter_)
        : iterator(iterator_)
        , object_storage(object_storage_)
        , context(context_)
        , filter(filter_)
    {
    }

    ObjectInfoPtr next(size_t items) override
    {
        while (true)
        {
            auto object = std::static_pointer_cast<IcebergDataObjectInfo>(iterator->next(items));
            if (!object)
                break;

            if (filter(object, context, object_storage))
                continue;
            return object;
        }
        return nullptr;
    }

    size_t estimatedKeysCount() override
    {
        return iterator->estimatedKeysCount();
    }

    std::optional<UInt64> getSnapshotVersion() const override
    {
        return iterator->getSnapshotVersion();
    }

private:
    ObjectIterator iterator;
    ObjectStoragePtr object_storage;
    ContextPtr context;
    FileFilter filter;
};

std::unordered_set<String> getOldFiles(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationPtr configuration)
{
    auto metadata_files = listFiles(*object_storage, *configuration, "metadata", "");
    auto data_files = listFiles(*object_storage, *configuration, "data", "");

    for (auto && data_file : data_files)
        metadata_files.push_back(data_file);

    return std::unordered_set<String>(metadata_files.begin(), metadata_files.end());
}

void clearOldFiles(ObjectStoragePtr object_storage, const std::unordered_set<String> & old_files)
{
    for (const auto & metadata_file : old_files)
    {
        object_storage->removeObjectIfExists(StoredObject(metadata_file));
    }
}

void compactIcebergTable(
    const StorageMetadataPtr & metadata_snapshot,
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationPtr configuration_,
    const std::optional<FormatSettings> & format_settings_,
    SharedHeader sample_block_,
    ContextPtr context_,
    std::shared_ptr<DataLake::ICatalog> catalog_,
    const StorageID & table_id_)
{
    auto old_files = getOldFiles(object_storage_, configuration_);

    {
        auto iterator = configuration_->getMetadata()->iterate(nullptr, {}, 0, metadata_snapshot, context_);
        auto large_files_iterator = std::make_shared<FilterFileIterator>(
            iterator,
            object_storage_,
            context_,
            [] (IcebergDataObjectInfoPtr object, ContextPtr context, ObjectStoragePtr object_storage) -> bool
        {
            return needToMoveFile(object, context, object_storage);
        });
        while (true)
        {
            auto large_file = large_files_iterator->next(0);
            if (!large_file)
                break;
            old_files.erase(large_file->getPath());
        }
    }
    auto columns_description = ColumnsDescription();
    for (size_t i = 0; i < sample_block_->columns(); ++i)
        columns_description.add(ColumnDescription(sample_block_->getNames()[i], sample_block_->getDataTypes()[i]));
    auto iterator = configuration_->getMetadata()->iterate(nullptr, {}, 0, metadata_snapshot, context_);
    auto large_files_iterator = std::make_shared<FilterFileIterator>(
        iterator,
        object_storage_,
        context_,
        [] (IcebergDataObjectInfoPtr object, ContextPtr context, ObjectStoragePtr object_storage) -> bool
    {
        return needToMoveFile(object, context, object_storage);
    });

    auto another_iterator = configuration_->getMetadata()->iterate(nullptr, {}, 0, metadata_snapshot, context_);
    auto small_files_iterator = std::make_shared<FilterFileIterator>(
        another_iterator,
        object_storage_,
        context_,
        [] (IcebergDataObjectInfoPtr object, ContextPtr context, ObjectStoragePtr object_storage) -> bool
    {
        return needToMoveFile(object, context, object_storage);
    });

    auto sink = std::make_shared<IcebergStorageSink>(object_storage_, configuration_, format_settings_, sample_block_, context_, catalog_, table_id_, false, large_files_iterator);

    auto read_from_format_info = configuration_->prepareReadingFromFormat(
        object_storage_,
        sample_block_->getNames(),
        me,
        true,
        /*supports_tuple_elements=*/ false,
        context_,
        {});

    auto source = StorageObjectStorageSource(
        "",
        object_storage_,
        configuration_,
        nullptr,
        read_from_format_info,
        format_settings_,
        context_,
        DBMS_DEFAULT_BUFFER_SIZE,
        small_files_iterator,
        nullptr,
        nullptr,
        false);

    while (true)
    {
        auto chunk = source.generate();
        if (chunk.getNumRows() == 0)
            break;
        std::cerr << "chunk params " << chunk.getNumRows() << ' ' << chunk.getNumColumns() << '\n';
        sink->consume(chunk);
    }
    sink->onFinish();
    clearOldFiles(object_storage_, old_files);
}

}

#endif
