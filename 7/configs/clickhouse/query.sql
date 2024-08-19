CREATE TABLE db.SHKonPlace
Engine = MergeTree
ORDER BY (shk_id)
AS
SELECT * FROM file('/var/lib/clickhouse/user_files/datasets/ttt.csv')
SETTINGS schema_inference_make_columns_nullable=0;
---
create table if not exists db.reports
(
    state_id char(3),
    qty      Int32,
    dt_date  timestamp,
    dt_load  timestamp
) engine = MergeTree order by (dt_load);
---
create or replace procedure sync.state_count_import(_src json)
    security definer
    language plpgsql
as
$$
begin
    insert into report.qty_state_id as ac(dt_date,
                                          state_id,
                                          qty,
                                          dt_load)
    select s.dt_date,
           s.state_id,
           s.qty,
           s.dt_load
    from json_to_recordset(_src) as s(dt_date  date,
                                      state_id char(3),
                                      qty      int,
                                      dt_load  timestamp)
    on conflict (dt_date, state_id) do update
        set qty     = excluded.qty,
            dt_load = excluded.dt_load
    where ac.dt_load <= excluded.dt_load;
end;
$$;