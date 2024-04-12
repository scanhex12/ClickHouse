-- https://github.com/ClickHouse/ClickHouse/issues/61947
SET allow_experimental_analyzer=1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.`1-1` (field Int8) ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`1-1` (field Int8) ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`2-1` (field Int8) ENGINE = Memory;

INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.`1-1` VALUES (1);

SELECT *
FROM {CLICKHOUSE_DATABASE:Identifier}.`1-1`
LEFT JOIN {CLICKHOUSE_DATABASE_1:Identifier}.`1-1` ON {CLICKHOUSE_DATABASE:Identifier}.`1-1`.field = {CLICKHOUSE_DATABASE_1:Identifier}.`1-1`.field;

SELECT '';

SELECT * FROM
(
SELECT 'using asterisk', {CLICKHOUSE_DATABASE:Identifier}.`1-1`.*, {CLICKHOUSE_DATABASE_1:Identifier}.`1-1`.*
FROM {CLICKHOUSE_DATABASE:Identifier}.`1-1`
LEFT JOIN {CLICKHOUSE_DATABASE_1:Identifier}.`1-1` USING field
UNION ALL
SELECT 'using field name', {CLICKHOUSE_DATABASE:Identifier}.`1-1`.field, {CLICKHOUSE_DATABASE_1:Identifier}.`1-1`.field
FROM {CLICKHOUSE_DATABASE:Identifier}.`1-1`
LEFT JOIN {CLICKHOUSE_DATABASE_1:Identifier}.`1-1` USING field
)
ORDER BY ALL;
