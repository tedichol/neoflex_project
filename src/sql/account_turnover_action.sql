CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(IN i_onDate DATE) AS $$
    BEGIN
        --очищаем записи за определенную дату
        DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_onDate;
        --вставляем новые
        INSERT INTO dm.dm_account_turnover_f
            WITH unique_accounts AS (
                SELECT DISTINCT
                    credit_account_rk AS account_rk
                FROM ds.ft_posting_f
                WHERE oper_date = i_onDate

                UNION
                
                SELECT DISTINCT
                    debet_account_rk as account_rk
                FROM ds.ft_posting_f
                WHERE oper_date = i_onDate
            ), credit_table AS (
                SELECT
                    credit_account_rk,
                    SUM(credit_amount) AS credit_amount
                FROM ds.ft_posting_f
                WHERE oper_date = i_onDate
                GROUP BY credit_account_rk
            ), debet_table AS (
                SELECT
                    debet_account_rk,
                    SUM(debet_amount) AS debet_amount
                FROM ds.ft_posting_f
                WHERE oper_date = i_onDate
                GROUP BY debet_account_rk
            ), exchange_rate AS (
                SELECT
                    un_ac.account_rk AS curr_account_rk,
                    COALESCE(ex_rat.reduced_cource, 1) AS cource
                FROM unique_accounts un_ac
                LEFT JOIN ds.md_account_d md_acc
                    ON un_ac.account_rk = md_acc.account_rk
                LEFT JOIN ds.md_exchange_rate_d ex_rat
                    ON md_acc.currency_rk = ex_rat.currency_rk
                        AND i_onDate BETWEEN ex_rat.data_actual_date AND ex_rat.data_actual_end_date
            ) SELECT
                i_onDate,
                un_ac.account_rk AS account_rk,
                COALESCE(ct.credit_amount, 0) AS credit_amount,
                COALESCE(ct.credit_amount, 0) * COALESCE(er.cource, 1) AS credit_amount_rub,
                COALESCE(dt.debet_amount, 0) AS debet_amount,
                COALESCE(dt.debet_amount, 0) * COALESCE(er.cource, 1) AS debet_amount_rub
            FROM unique_accounts un_ac
            LEFT JOIN credit_table ct
                ON un_ac.account_rk = ct.credit_account_rk
            LEFT JOIN debet_table dt
                ON un_ac.account_rk = dt.debet_account_rk
            LEFT JOIN exchange_rate er
                ON un_ac.account_rk = er.curr_account_rk;
        --логирование
        INSERT INTO logs.proc_calc_logs (time_point, specification)
            VALUES (NOW(), 'Расчет account_turnover за ' || i_onDate || '. Строк добавлено: ' ||
                (SELECT COUNT(*) FROM dm.dm_account_turnover_f WHERE on_date = i_onDate));
    END;
$$ LANGUAGE plpgsql