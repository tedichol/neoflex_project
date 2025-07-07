import pandas as pd
from sqlalchemy import (Table, Column, Float, Integer, Numeric,
                        Date, String, PrimaryKeyConstraint, CHAR)

def create_log_tables(metadata) -> dict[str, pd.DataFrame]:
     return {"logs": Table(
        "logs",
        metadata,
        Column("start_time", Date,),
        Column("Description", String),
    )
    }

def create_data_tables(metadata) -> dict[str, pd.DataFrame]:
    return {"ft_balance_f": Table(
        "ft_balance_f",
        metadata,
        Column("on_date", Date, nullable=False),
        Column("account_rk", Numeric, nullable=False),
        Column("currency_rk", Numeric),
        Column("balance_out", Float),
        PrimaryKeyConstraint("on_date", "account_rk", name="ft_balance_pk"),
    ),
     "ft_posting_f": Table(
        "ft_posting_f",
        metadata,
        Column("oper_date", Date, nullable=False),
        Column("credit_account_rk", Numeric, nullable=False),
        Column("debet_account_rk", Numeric, nullable=False),
        Column("credit_amount", Float),
        Column("debet_amount", Float),
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
    )
}