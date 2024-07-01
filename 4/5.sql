--
create table staging.table
(
    shk_id    UInt64,
    office_id UInt64,
    is_stock  Boolean,
    dt_load   DateTime
)
engine = MergeTree
order by shk_id;

--
create table log.buffer
(
    shk_id    UInt64,
    office_id UInt64,
    is_stock  Boolean,
    dt_load   DateTime
)
engine = Buffer(staging, table, 16, 10, 100, 10000, 1000000, 10000000, 100000000);