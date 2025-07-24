import csv
import os
import time
from sqlalchemy import MetaData, create_engine, text
from db_str_config import db_str_config
from tables import create_exp_tables

# директория и файл для выгрузки
export_dir = "export_files"
file_name = "feb_f101"

# Создаем директорию, если ее нет
os.makedirs(export_dir, exist_ok=True)

engine = create_engine(url=db_str_config("db_conn_params.txt"),)

metadata = MetaData()
exp_tables = create_exp_tables(metadata)
metadata.create_all(engine)

with (engine.connect() as cursor_conn,
      engine.connect() as simple_conn,
    open(f"{export_dir}/{file_name}.csv", "w", newline='', encoding="utf-8") as export_file):
    simple_conn.execute(text(f'DELETE FROM logs.{exp_tables["export_logs"].name}'))
    simple_conn.execute(text(f'INSERT INTO logs.{exp_tables["export_logs"].name} VALUES (NOW(), '
                      f"'Соединение установлено, директория и файл созданы, "
                      f"начинается процесс выгрузки 101-ой формы')"))
    simple_conn.commit()
    writer = csv.writer(export_file, delimiter=';')
    # в данный момент это "серверный" курсор
    cursor_result = cursor_conn.execution_options(stream_results=True,
        max_row_buffer=10).execute(text("SELECT * FROM dm.dm_f101_round_f"))
    writer.writerow(cursor_result.keys())
    batch_size = 5
    while True:
        batch = cursor_result.fetchmany(batch_size)
        if not batch: break
        else: writer.writerows(batch)
        simple_conn.execute(text(f'INSERT INTO logs.{exp_tables["export_logs"].name} VALUES (NOW(), '
                          f"'Выгружено {len(batch)} строк')"))
        time.sleep(1)
        simple_conn.commit()
    simple_conn.execute(text(f'INSERT INTO logs.{exp_tables["export_logs"].name} VALUES (NOW(), '
                      f"'Выгрузка закончена')"))
    simple_conn.commit()