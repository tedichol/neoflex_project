import logging
import time
from sqlalchemy import *
from sqlalchemy.dialects.postgresql import insert as need_insert
import pandas as pd

#настройка логирования 
logging.getLogger('sqlalchemy.engine').setLevel(logging.ERROR)

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

df_tables_pk = {"ft_balance_f": ["on_date", "account_rk"],
                "ft_posting_f": [],
                "md_account_d":["data_actual_date", "account_rk"],
                "md_currency_d":["currency_rk", "data_actual_date"],
                "md_exchange_rate_d":["data_actual_date", "currency_rk"],
                "md_ledger_account_s":["ledger_account", "start_date"]}

engine = create_engine("postgresql+psycopg2://ds:1234@localhost:5432/staff",)

metadata_obj = MetaData()

ft_balance_pyvar = Table(
    "ft_balance_f",
    metadata_obj,
    Column("on_date", Date, nullable=False),
    Column("account_rk", Numeric, nullable=False),
    Column("currency_rk", Numeric),
    Column("balance_out", Float),
    PrimaryKeyConstraint("on_date", "account_rk", name="ft_balance_pk"),
)
ft_posting_pyvar = Table(
    "ft_posting_f",
    metadata_obj,
    Column("oper_date", Date, nullable=False),
    Column("credit_account_rk", Numeric, nullable=False),
    Column("debet_account_rk", Numeric, nullable=False),
    Column("credit_amount", Float),
    Column("debet_amount", Float),
)
md_account_pyvar = Table(
    "md_account_d",
    metadata_obj,
    Column("data_actual_date", Date, nullable=False),
    Column("data_actual_end_date", Date, nullable=False),
    Column("account_rk", Numeric, nullable=False),
    Column("account_number", String(20), nullable=False),
    Column("char_type", String(1), nullable=False),
    Column("currency_rk", Numeric, nullable=False),
    Column("currency_code", String(3), nullable=False),
    PrimaryKeyConstraint("data_actual_date", "account_rk", name="md_account_pk"),
)
md_currency_pyvar = Table(
    "md_currency_d",
    metadata_obj,
    Column("currency_rk", Numeric, nullable=False),
    Column("data_actual_date", Date, nullable=False),
    Column("data_actual_end_date", Date),
    Column("currency_code", String(3)),
    Column("code_iso_char", String(3)),
    PrimaryKeyConstraint("currency_rk", "data_actual_date", name="md_currency_pk"),
)
md_exchange_rate_pyvar = Table(
    "md_exchange_rate_d",
    metadata_obj,
    Column("data_actual_date", Date, nullable=False),
    Column("data_actual_end_date", Date),
    Column("currency_rk", Numeric, nullable=False),
    Column("reduced_cource", Float),
    Column("code_iso_num", String(3)),
    PrimaryKeyConstraint("data_actual_date", "currency_rk", name="md_exchange_rate_pk"),
)
md_ledger_account_pyvar = Table(
    "md_ledger_account_s",
    metadata_obj,
    Column("chapter", CHAR(1)),
    Column("chapter_name", String(16)),
    Column("section_number", Integer),
    Column("section_name", String(22)),
    Column("subsection_name", String(21)),
    Column("ledger_account1", Integer),
    Column("ledger_account1_name", String(47)),
    Column("ledger_account", Integer, nullable=False),
    Column("ledger_account_name", String(153)),
    Column("characteristic", CHAR(1)),
    Column("is_resident", Integer),
    Column("is_reserve", Integer),
    Column("is_reserved", Integer),
    Column("is_loan", Integer),
    Column("is_reserved_assets", Integer),
    Column("is_overdue", Integer),
    Column("is_interest", Integer),
    Column("pair_account", String(5)),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date),
    Column("is_rub_only", Integer),
    Column("min_term", String(1)),
    Column("min_term_measure", String(1)),
    Column("max_term", String(1)),
    Column("max_term_measure", String(1)),
    Column("ledger_acc_full_name_translit", String(1)),
    Column("is_revaluation", String(1)),
    Column("is_correct", String(1)),
    PrimaryKeyConstraint("ledger_account", "start_date", name="md_ledger_account_pk"),
)

#вспомогательная функция
def upsert(table, connection, keys, data_iter):
    try:
        data = [dict(zip(keys, row)) for row in data_iter]
        stmt = need_insert(table.table).values(data)
        stmt = stmt.on_conflict_do_update(
                index_elements = df_tables_pk[table.table.name],
                set_ = {col: stmt.excluded[col] for col in keys if col != 'id'}
            )
        connection.execute(stmt)
    except Exception as e:
        print(f"Ошибка: {e}")
        raise

#запись таблиц
with engine.connect() as conn:
    for (df, table_name) in df_table_list:
        df.to_sql(table_name, conn,
                       if_exists='append', index=False, method=upsert)
        print(f"записали таблицу {table_name}")
        conn.commit()
    conn.close()

#запись для таблицы без первичного ключа
with engine.begin() as conn:
    #очищаем
    conn.execute(text("TRUNCATE TABLE ds.ft_posting_f"))
    #записываем
    df_ft_posting.to_sql("ft_posting_f", conn, if_exists="append", index=False)
    print("записали таблицу ft_posting_f")

#таймер на 5 секунд
time.sleep(5)
