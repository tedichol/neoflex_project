from datetime import date, timedelta
import time
from sqlalchemy import DDL, MetaData, create_engine, text
from db_str_config import db_str_config
from tables import create_dm_tables

engine = create_engine(url=db_str_config("db_conn_params.txt"),)

# создание схемы
with engine.begin() as conn:
    conn.execute(DDL("CREATE SCHEMA IF NOT EXISTS dm"))

metadata = MetaData()
dm_tables = create_dm_tables(metadata)

# создание таблиц
metadata.create_all(engine)

# создание процедур
with (engine.begin() as conn,
      open("src/sql/account_turnover_action.sql", "r", encoding="utf-8") as acc_tur_proc,
      open("src/sql/account_balance_init.sql", "r", encoding="utf-8") as acc_bal_init,
      open("src/sql/account_balance_action.sql", "r", encoding="utf-8") as acc_bal_proc):
    conn.execute(text(acc_tur_proc.read()))
    conn.execute(text(acc_bal_init.read()))
    conn.execute(text(acc_bal_proc.read()))

# создание списка дат
start_date = date(2018, 1, 1)
end_date = date(2018, 1, 31)
list_date = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

# выполнение процедур
with engine.connect() as conn:
    # очищение логов процедур
    conn.execute(text("TRUNCATE TABLE logs.proc_calc_logs"))
    conn.commit()
    # расчет витрины оборотов
    for in_date in list_date:
        conn.execute(text("CALL ds.fill_account_turnover_f(:date)"), {"date": in_date})
        time.sleep(0.5)
        conn.commit()
    # инициализация витрины остатков
    conn.execute(text('CALL ds.init_account_balance_f()'))
    time.sleep(0.5)
    conn.commit()
    # расчет витрины остатков
    for in_date in list_date:
        conn.execute(text("CALL ds.fill_account_balance_f(:date)"), {"date": in_date})
        time.sleep(0.5)
        conn.commit()