--
insert into
    log.buffer(shk_id, office_id, dt_load, is_stock)
VALUES
    (123456, 372, '2024-06-28 12:05:12', false),
    (345216, 123, '2024-06-28 12:09:12', true),
    (123425, 332, '2024-06-28 12:10:12', true),
    (122231, 123, '2024-06-28 12:11:12', false),
    (222322, 223, '2024-06-28 12:34:12', true),
    (332231, 372, '2024-06-28 12:43:12', true);

--
select
    *
from
    staging.table;

--
select
    *
from
    current.table;