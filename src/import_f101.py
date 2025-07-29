import csv
import time
from sqlalchemy import MetaData, create_engine, text
from db_str_config import db_str_config
from tables import create_imp_tables


# директория и файл для считывания
import_dir = "import_files"
file_name = "feb_f101"

# название новой таблицы
imp_table_title = "dm_f101_round_f_v2"

def iter_read_file():
    with open(f"{import_dir}/{file_name}.csv", "r", newline='', encoding="utf-8") as import_file:
        reader = csv.reader(import_file, delimiter=';')
        next(reader)
        for row in reader:
            yield row

def iter_col_assign(col_values):
    for i in col_values:
        yield i

engine = create_engine(url=db_str_config("db_conn_params.txt"),)

metadata = MetaData()
imp_tables = create_imp_tables(metadata, imp_table_title)
metadata.create_all(engine)

with engine.connect() as conn:
    conn.execute(text(f'DELETE FROM logs.{imp_tables["import_logs"].name}'))
    conn.execute(text(f"INSERT INTO logs.{imp_tables['import_logs'].name} VALUES (NOW(), '"
                      f"Начинается извлечение и загрузка данных')"))
    conn.commit()
    i = 0
    for row in iter_read_file():
       iter_col_vals = iter_col_assign(row)
       query = text(f'INSERT INTO dm.{imp_table_title} VALUES (:from_date, :to_date, :chapter,'
                         f":ledger_account, :characteristic, :balance_in_rub, :balance_in_val,"
                         f":balance_in_total, :turn_deb_rub, :turn_deb_val, :turn_deb_total,"
                         f":turn_cre_rub, :turn_cre_val, :turn_cre_total, :balance_out_rub,"
                         f":balance_out_val, :balance_out_total)")
       params = {"from_date":next(iter_col_vals), "to_date": next(iter_col_vals),
                          "chapter": next(iter_col_vals), "ledger_account": next(iter_col_vals),
                         "characteristic": next(iter_col_vals), "balance_in_rub": next(iter_col_vals),
                         "balance_in_val": next(iter_col_vals), "balance_in_total": next(iter_col_vals),
                         "turn_deb_rub": next(iter_col_vals), "turn_deb_val": next(iter_col_vals),
                         "turn_deb_total": next(iter_col_vals), "turn_cre_rub": next(iter_col_vals),
                         "turn_cre_val": next(iter_col_vals), "turn_cre_total": next(iter_col_vals),
                         "balance_out_rub": next(iter_col_vals), "balance_out_val": next(iter_col_vals),\
                         "balance_out_total": next(iter_col_vals)}
       conn.execute(query, params)
       conn.execute(text(f"INSERT INTO logs.{imp_tables['import_logs'].name} VALUES (NOW(), '"
                      f"Записана :i-ая строка данных')"), {"i": i})
       time.sleep(0.5)
       conn.commit()
       i += 1
    conn.execute(text(f"INSERT INTO logs.{imp_tables['import_logs'].name} VALUES (NOW(), '"
                      f"Загрузка данных завершена')"))
    conn.commit()
