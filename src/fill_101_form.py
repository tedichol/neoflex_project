from datetime import date
import time
from sqlalchemy import MetaData, create_engine, text
from db_str_config import db_str_config
from tables import create_101f_tables

engine = create_engine(url=db_str_config("db_conn_params.txt"),)

# создание таблиц
metadata = MetaData()
create_101f_tables(metadata)
metadata.create_all(engine)

# создание процедуры
with (engine.begin() as conn,
      open("src/sql/f101_round_action.sql", "r", encoding="utf-8") as f101_calc):
    conn.execute(text(f101_calc.read()))

# отчетная дата
in_date = date(2018, 2, 1)

# выполнение процедур
with engine.connect() as conn:
    # очищение логов выполнения процедуры
    conn.execute(text("TRUNCATE TABLE logs.f101_proc_logs"))
    time.sleep(5)
    conn.commit()
    # расчет 101-ой формы
    conn.execute(text("CALL dm.fill_f101_round_f(:date)"), {"date": in_date})
    time.sleep(5)
    conn.commit()