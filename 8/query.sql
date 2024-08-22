select
  toStartOfDay(parseDateTimeBestEffortOrNull(dt)),
  uniq(shk_id) qty_shk,
  uniq(tare_id) qty_tare,
  countIf(shk_id, tare_id ='0') qty_shk_without_tare,
  countIf(shk_id, isstock = '1') qty_shk_stock,
  countIf(shk_id, state_id = 'WWE') qty_shk_wwe,
  countIf(shk_id, place_cod = '0') qty_shk_without_pc
from default.SHKonPlace
group by 1;

//по дате условие не ставил, так как смотрю за все время
//но если оно требуется, то можно добавить where dt >= today() - interval "Кол-во дней" day