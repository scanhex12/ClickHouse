#include <Processors/Transforms/PlanSquashingTransform.h>
#include <Processors/IProcessor.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PlanSquashingTransform::PlanSquashingTransform(const Block & header, size_t min_block_size_rows, size_t min_block_size_bytes, size_t num_ports)
    : IProcessor(InputPorts(num_ports, header), OutputPorts(num_ports, header)), balance(header, min_block_size_rows, min_block_size_bytes)
{
}

IProcessor::Status PlanSquashingTransform::prepare()
{
    Status status = Status::Ready;

    while (planning_status != PlanningStatus::FINISH)
    {
        switch (planning_status)
        {
            case INIT:
            {
                status = init();
                break;
            }
            case READ_IF_CAN:
            {
                status = prepareConsume();
                break;
            }
            case PUSH:
            {
                status = push();
                break;
            }
            case WAIT_IN:
                return waitForDataIn();
            case WAIT_OUT:
                return prepareSend();
            case WAIT_OUT_FLUSH:
                return prepareSendFlush();
            case FINISH:
                break; /// never reached
        }
    }
    status = finish();

    return status;
}

void PlanSquashingTransform::work()
{
    prepare();
}

IProcessor::Status PlanSquashingTransform::init()
{
    for (auto input : inputs)
    {
        input.setNeeded();
        if (input.hasData())
            available_inputs++;
    }

    planning_status = PlanningStatus::READ_IF_CAN;
    return Status::Ready;
}

IProcessor::Status PlanSquashingTransform::prepareConsume()
{
    if (available_inputs == 0)
    {
        planning_status = PlanningStatus::WAIT_IN;
        return Status::NeedData;
    }
    finished = false;

    bool inputs_have_no_data = true;
    for (auto & input : inputs)
    {
        if (input.hasData())
        {
            inputs_have_no_data = false;
            chunk = input.pull();
            transform(chunk);

            available_inputs--;
            if (chunk.hasChunkInfo())
            {
                planning_status = PlanningStatus::WAIT_OUT;
                return Status::Ready;
            }
        }

        if (available_inputs == 0)
        {
            planning_status = PlanningStatus::WAIT_IN;
            return Status::NeedData;
        }
    }

    if (inputs_have_no_data)
    {
        if (checkInputs())
            return Status::Ready;

        planning_status = PlanningStatus::WAIT_IN;
        return Status::NeedData;
    }
    return Status::Ready;
}

bool PlanSquashingTransform::checkInputs()
{
    bool all_finished = true;
    for (auto & input : inputs)
        if (!input.isFinished())
            all_finished = false;

    if (all_finished) /// If all inputs are closed, we check if we have data in balancing
    {
        if (balance.isDataLeft()) /// If we have data in balancing, we process this data
        {
            planning_status = PlanningStatus::WAIT_OUT_FLUSH;
            finished = true;
            transform(chunk);
        }
        // else    /// If we don't have data, We send FINISHED
        //     planning_status = PlanningStatus::FINISH;
        return true;
    }
    return false;
}

bool PlanSquashingTransform::checkOutputs()
{
    bool all_finished = true;

    for (auto & output : outputs)
        if (!output.isFinished())
            all_finished = false;

    if (all_finished) /// If all outputs are closed, we close inputs (just in case)
    {
        planning_status = PlanningStatus::FINISH;
        return true;
    }
    return false;
}

IProcessor::Status PlanSquashingTransform::waitForDataIn()
{
    bool all_finished = true;
    for (auto & input : inputs)
    {
        if (input.isFinished())
            continue;

        all_finished = false;

        if (!input.hasData())
            continue;

        available_inputs++;
    }
    if (all_finished)
    {
        checkInputs();
        return Status::Ready;
    }

    if (available_inputs > 0)
    {
        planning_status = PlanningStatus::READ_IF_CAN;
        return Status::Ready;
    }

    return Status::NeedData;
}

void PlanSquashingTransform::transform(Chunk & chunk_)
{
    if (!finished)
    {
        Chunk res_chunk = balance.add(std::move(chunk_));
        std::swap(res_chunk, chunk_);
    }
    else
    {
        Chunk res_chunk = balance.add({});
        std::swap(res_chunk, chunk_);
    }
}

IProcessor::Status PlanSquashingTransform::push()
{
    if (!free_output)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There should be a free output in push()");

    if (finished)
        planning_status = PlanningStatus::FINISH;
    else
        planning_status = PlanningStatus::READ_IF_CAN;

    free_output->push(std::move(chunk));
    free_output = nullptr;
    return Status::Ready;
}

IProcessor::Status PlanSquashingTransform::prepareSend()
{
    if (!chunk)
    {
        planning_status = PlanningStatus::FINISH;
        return Status::Ready;
    }

    for (auto &output : outputs)
    {
        if (output.canPush())
        {
            planning_status = PlanningStatus::PUSH;
            free_output = &output;
            return Status::Ready;
        }
    }
    return Status::PortFull;
}

IProcessor::Status PlanSquashingTransform::prepareSendFlush()
{
    if (!chunk)
    {
        planning_status = PlanningStatus::FINISH;
        return Status::Ready;
    }

    for (auto &output : outputs)
    {

        if (output.canPush())
        {
            planning_status = PlanningStatus::PUSH;
            free_output = &output;
            return Status::Ready;
        }
    }
    return Status::PortFull;
}

IProcessor::Status PlanSquashingTransform::finish()
{
    for (auto & in : inputs)
        in.close();
    for (auto & output : outputs)
        output.finish();

    return Status::Finished;
}
}
