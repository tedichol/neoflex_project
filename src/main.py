import time
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import insert as pg_insert
import pandas as pd
from tables import create_data_tables, create_log_tables

#читаем файлы в dataframe
df_ft_balance = pd.read_csv("files_for_read/ft_balance_f.csv", sep=";")
df_ft_balance.columns = df_ft_balance.columns.str.lower()

df_ft_posting = pd.read_csv("files_for_read/ft_posting_f.csv", sep=";")
df_ft_posting.columns = df_ft_posting.columns.str.lower()

df_md_account = pd.read_csv("files_for_read/md_account_d.csv", sep=";")
df_md_account.columns = df_md_account.columns.str.lower()

#без указания кодировки ошибка чтения
df_md_currency = pd.read_csv("files_for_read/md_currency_d.csv", sep=";", encoding="cp1252")
df_md_currency.columns = df_md_currency.columns.str.lower()
df_md_currency = df_md_currency.replace(b"\x98".decode("cp1252"), "NoV", regex=False)
df_md_currency = df_md_currency.fillna({"currency_code": "-1", "code_iso_char": "NoV"}) # No Value
df_md_currency["currency_code"] = df_md_currency["currency_code"].astype(int)

df_md_exchange_rate = pd.read_csv("files_for_read/md_exchange_rate_d.csv", sep=";")
df_md_exchange_rate.columns = df_md_exchange_rate.columns.str.lower()
df_md_exchange_rate = df_md_exchange_rate.drop_duplicates()

df_md_ledger_account = pd.read_csv("files_for_read/md_ledger_account_s.csv", sep=";")
df_md_ledger_account.columns = df_md_ledger_account.columns.str.lower()

#список с таблицами, имеющими первичный ключ
df_list = [df_ft_balance, df_md_account,
           df_md_currency, df_md_exchange_rate, df_md_ledger_account]
table_list = ["ft_balance_f", "md_account_d",
           "md_currency_d", "md_exchange_rate_d", "md_ledger_account_s"]

df_table_list = zip(df_list, table_list)
'''
df_tables_pk = {"ft_balance_f": ["on_date", "account_rk"],
                "ft_posting_f": [],
                "md_account_d":["data_actual_date", "account_rk"],
                "md_currency_d":["currency_rk", "data_actual_date"],
                "md_exchange_rate_d":["data_actual_date", "currency_rk"],
                "md_ledger_account_s":["ledger_account", "start_date"]}
'''

data_metadata_obj = MetaData()
data_tables = create_data_tables(data_metadata_obj)
log_metadata_obj = MetaData()
log_tables = create_log_tables(log_metadata_obj)

engine = create_engine("postgresql+psycopg2://ds:1234@localhost:5432/staff",)
'''
#вспомогательная функция
def upsert(table, connection, keys, data_iter):
    try:
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = pg_insert(table.table).values(data)
        stmt = stmt.on_conflict_do_update(
                index_elements = df_tables_pk[table.table.name],
                set_ = {col: stmt.excluded[col] for col in keys if col != 'id'}
            )
        connection.execute(stmt)
    except Exception as e:
        print(f"Ошибка: {e}")
        raise
'''
#очищаем таблицу логов
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE logs.logs"))

#запись таблиц
with engine.connect() as conn:
    conn.execute(text("INSERT INTO logs.logs VALUES (LOCALTIMESTAMP, 'Таблицы считаны и обработаны')"))
    for (df, table_name) in df_table_list:
        conn.execute(text(f"INSERT INTO logs.logs VALUES (LOCALTIMESTAMP, 'Началось заполнение таблицы {table_name}')"))
        table = data_tables[table_name]
        data = df.to_dict("records")
        conflict_columns = [col.name for col in table.primary_key.columns]
        for record in data:
            db_insert = pg_insert(table).values(record).on_conflict_do_update(
                index_elements = conflict_columns,
                set_ = {k: v for k, v in record.items() if k not in conflict_columns})
            conn.execute(db_insert)
        conn.execute(text(f"INSERT INTO logs.logs VALUES (LOCALTIMESTAMP, 'Таблица {table_name} заполнена')"))
        conn.commit()

#запись для таблицы без первичного ключа
with engine.begin() as conn:
    conn.execute(text("TRUNCATE TABLE ds.ft_posting_f"))
    conn.execute(text(f"INSERT INTO logs.logs VALUES (LOCALTIMESTAMP, 'Началось заполнение таблицы ds.ft_posting_f')"))
    table = data_tables["ft_posting_f"]
    data = df_ft_posting.to_dict("records")
    db_insert = pg_insert(table).values(data)
    conn.execute(db_insert)
    conn.execute(text("INSERT INTO logs.logs VALUES (LOCALTIMESTAMP, 'Таблица ds.ft_posting_f заполнена')"))

#таймер на 5 секунд
time.sleep(5)

with engine.begin() as conn:
    conn.execute(text(f"INSERT INTO logs.logs VALUES (LOCALTIMESTAMP, 'Загрузка завершена')"))
