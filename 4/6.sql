create table current.table
(
    office_id      UInt64,
    is_stock_count SimpleAggregateFunction(sum, UInt64),
    total          SimpleAggregateFunction(sum, UInt64)
)
engine AggregatingMergeTree
order by office_id;

create materialized view staging.mv_table
to current.table
as
select 
    office_id,
    countIf(is_stock = 1) as is_stock_count,
    count() AS total
from 
    staging.table
group by 
    office_id;