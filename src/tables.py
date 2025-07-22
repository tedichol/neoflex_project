import pandas as pd
from sqlalchemy import (Table, Column, Float, Integer, Numeric,
                        Date, String, PrimaryKeyConstraint, CHAR, TIMESTAMP)

def create_log_tables(metadata) -> dict[str, pd.DataFrame]:
     return {"logs": Table(
        "logs",
        metadata,
        Column("time_point", TIMESTAMP),
        Column("Description", String,),
        schema="logs"
    )
    }

def create_ds_data_tables(metadata) -> dict[str, pd.DataFrame]:
    return {"ft_balance_f": Table(
        "ft_balance_f",
        metadata,
        Column("on_date", Date, nullable=False),
        Column("account_rk", Numeric, nullable=False),
        Column("currency_rk", Numeric),
        Column("balance_out", Float),
        PrimaryKeyConstraint("on_date", "account_rk", name="ft_balance_pk"),
        schema="ds"
    ),
     "ft_posting_f": Table(
        "ft_posting_f",
        metadata,
        Column("oper_date", Date, nullable=False),
        Column("credit_account_rk", Numeric, nullable=False),
        Column("debet_account_rk", Numeric, nullable=False),
        Column("credit_amount", Float),
        Column("debet_amount", Float),
        schema="ds"
    ),
    "md_account_d": Table(
        "md_account_d",
        metadata,
        Column("data_actual_date", Date, nullable=False),
        Column("data_actual_end_date", Date, nullable=False),
        Column("account_rk", Numeric, nullable=False),
        Column("account_number", String(20), nullable=False),
        Column("char_type", String(1), nullable=False),
        Column("currency_rk", Numeric, nullable=False),
        Column("currency_code", String(3), nullable=False),
        PrimaryKeyConstraint("data_actual_date", "account_rk", name="md_account_pk"),
        schema="ds"
    ),
    "md_currency_d": Table(
        "md_currency_d",
        metadata,
        Column("currency_rk", Numeric, nullable=False),
        Column("data_actual_date", Date, nullable=False),
        Column("data_actual_end_date", Date),
        Column("currency_code", String(3)),
        Column("code_iso_char", String(3)),
        PrimaryKeyConstraint("currency_rk", "data_actual_date", name="md_currency_pk"),
        schema="ds"
    ),
    "md_exchange_rate_d": Table(
        "md_exchange_rate_d",
        metadata,
        Column("data_actual_date", Date, nullable=False),
        Column("data_actual_end_date", Date),
        Column("currency_rk", Numeric, nullable=False),
        Column("reduced_cource", Float),
        Column("code_iso_num", String(3)),
        PrimaryKeyConstraint("data_actual_date", "currency_rk", name="md_exchange_rate_pk"),
        schema="ds"
    ),
    "md_ledger_account_s": Table(
        "md_ledger_account_s",
        metadata,
        Column("chapter", CHAR(1)),
        Column("chapter_name", String(16)),
        Column("section_number", Integer),
        Column("section_name", String(22)),
        Column("subsection_name", String(21)),
        Column("ledger1_account", Integer),
        Column("ledger1_account_name", String(47)),
        Column("ledger_account", Integer, nullable=False),
        Column("ledger_account_name", String(153)),
        Column("characteristic", CHAR(1)),
        Column("start_date", Date, nullable=False),
        Column("end_date", Date),
        PrimaryKeyConstraint("ledger_account", "start_date", name="md_ledger_account_pk"),
        schema="ds"
    )
    }

def create_dm_tables(metadata) -> dict[str, Table]:
     return {"dm_account_turnover_f": Table(
          "dm_account_turnover_f",
          metadata,
          Column("on_date", Date),
          Column("account_rk", Numeric),
          Column("credit_amount", Numeric(23,8)),
          Column("credit_amount_rub", Numeric(23,8)),
          Column("debet_amount", Numeric(23,8)),
          Column("debet_amount_rub", Numeric(23,8)),
          schema="dm",
     ),
     "dm_account_balance_f": Table(
          "dm_account_balance_f",
          metadata,
          Column("on_date", Date),
          Column("account_rk", Numeric),
          Column("balance_out", Numeric(23, 8)),
          Column("balance_out_rub", Numeric(23, 8)),
          schema="dm",
     ),
     "proc_calc_logs":Table(
        "proc_calc_logs",
        metadata,
        Column("time_point", TIMESTAMP),
        Column("specification", String),
        schema="logs",
     ),
     }

# разделяй таблицы по надобности/заданию/времени, а не назначению и создавай исходя из него,
# как create_dm_tables, а не предыдущие

def create_101f_tables(metadata) -> None:
     Table(
          "dm_f101_round_f",
          metadata,
          Column("from_date", Date),
          Column("to_date", Date),
          Column("chapter", CHAR(1)),
          Column("ledger_account", CHAR(5)),
          Column("characteristic", CHAR(1)),
          Column("balance_in_rub", Numeric(23, 8)),
          Column("balance_in_val", Numeric(23, 8)),
          Column("balance_in_total", Numeric(23, 8)),
          Column("turn_deb_rub", Numeric(23, 8)),
          Column("turn_deb_val", Numeric(23, 8)),
          Column("turn_deb_total", Numeric(23, 8)),
          Column("turn_cre_rub", Numeric(23, 8)),
          Column("turn_cre_val", Numeric(23, 8)),
          Column("turn_cre_total", Numeric(23, 8)),
          Column("balance_out_rub", Numeric(23, 8)),
          Column("balance_out_val", Numeric(23, 8)),
          Column("balance_out_total", Numeric(23, 8)),
          schema="dm",)
     Table(
          "f101_proc_logs",
          metadata,
          Column("time_point", TIMESTAMP),
          Column("specification", String),
          schema="logs"
     )
