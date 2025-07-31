import csv
from sqlalchemy import create_engine, text
from db_str_config import db_str_config


# директория и файл для считывания
import_dir = "project_task_2/input_for_task/data/dict_currency"
file_name = "dict_currency"

# таблица назначения
schema = "dm"
imp_table_title = "dict_currency"

def iter_read_file():
    with open(f"{import_dir}/{file_name}.csv", "r", newline='') as import_file:
        reader = csv.reader(import_file, delimiter=',')
        next(reader)
        for row in reader:
            yield row

def iter_col_assign(col_values):
    for i in col_values:
        yield i

engine = create_engine(url=db_str_config("db_conn_params.txt"),)

#metadata = MetaData()
#imp_tables = create_imp_tables(metadata, imp_table_title)
#metadata.create_all(engine)

with engine.connect() as conn:
    #conn.execute(text(f'DELETE FROM logs.{imp_tables["import_logs"].name}'))
    #conn.execute(text(f"INSERT INTO logs.{imp_tables['import_logs'].name} VALUES (NOW(), '"
    #                  f"Начинается извлечение и загрузка данных')"))
    #conn.commit()
    #i = 0
    for row in iter_read_file():
       iter_col_vals = iter_col_assign(row)
       query = text(f'INSERT INTO {schema}.{imp_table_title} VALUES (:currency_cd, :currency_name, ' #, :deal_name,'
                         # f":deal_sum, :client_rk, :account_rk, :agreement_rk,"
                         # f":deal_start_date, :department_rk, :product_rk, :deal_type_cd,"
                         f":effective_from_date, :effective_to_date" #, :turn_cre_total, :balance_out_rub,"
                         #f":balance_out_val, :balance_out_total
                         f")"
                         )
       params = {"currency_cd":next(iter_col_vals), "currency_name": next(iter_col_vals),
                         #"deal_name": next(iter_col_vals), "deal_sum": next(iter_col_vals),
                         #"client_rk": next(iter_col_vals), "account_rk": next(iter_col_vals),
                         #"agreement_rk": next(iter_col_vals), "deal_start_date": next(iter_col_vals),
                         #"department_rk": next(iter_col_vals), "product_rk": next(iter_col_vals),
                         #"deal_type_cd": next(iter_col_vals),
                         "effective_from_date": next(iter_col_vals),
                         "effective_to_date": next(iter_col_vals) #, "turn_cre_total": next(iter_col_vals),
                         #"balance_out_rub": next(iter_col_vals), "balance_out_val": next(iter_col_vals),\
                         #"balance_out_total": next(iter_col_vals)
                         }
       conn.execute(query, params)
       #conn.execute(text(f"INSERT INTO logs.{imp_tables['import_logs'].name} VALUES (NOW(), '"
       #               f"Записана :i-ая строка данных')"), {"i": i})
       #time.sleep(0.5)
       conn.commit()
       #i += 1
    #conn.execute(text(f"INSERT INTO logs.{imp_tables['import_logs'].name} VALUES (NOW(), '"
    #                  f"Загрузка данных завершена')"))
    #conn.commit()
