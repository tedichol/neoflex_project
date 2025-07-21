CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(IN i_onDate DATE) AS $$
    BEGIN
        DELETE FROM dm.dm_account_balance_f WHERE on_date = i_onDate;
        INSERT INTO dm.dm_account_balance_f
            WITH current_accounts AS (
                SELECT
                    account_rk,
                    char_type
                FROM ds.md_account_d md_acc
                WHERE i_onDate BETWEEN md_acc.data_actual_date AND md_acc.data_actual_end_date
            ), prev_day_remains AS (
                SELECT *
                FROM dm.dm_account_balance_f
                WHERE on_date = i_onDate - interval '1 day'
            ), active_accounts AS (
                SELECT
                    ca.account_rk,
                    COALESCE(pdr.balance_out, 0) + SUM(COALESCE(dm_acc_turn.debet_amount, 0))
                        - SUM(COALESCE(dm_acc_turn.credit_amount, 0)) AS balance_out,
                    COALESCE(pdr.balance_out_rub, 0) + SUM(COALESCE(dm_acc_turn.debet_amount_rub, 0))
                        - SUM(COALESCE(dm_acc_turn.credit_amount_rub, 0)) AS balance_out_rub
                FROM current_accounts ca
                LEFT JOIN prev_day_remains pdr
                    ON ca.account_rk = pdr.account_rk
                LEFT JOIN dm.dm_account_turnover_f dm_acc_turn
                    ON ca.account_rk = dm_acc_turn.account_rk
                        AND dm_acc_turn.on_date = i_onDate
                WHERE char_type = 'А'--русский символ
                GROUP BY ca.account_rk, pdr.balance_out, pdr.balance_out_rub
            ), passive_accounts AS (
                SELECT
                    ca.account_rk,
                    COALESCE(pdr.balance_out, 0) - SUM(COALESCE(dm_acc_turn.debet_amount, 0))
                        + SUM(COALESCE(dm_acc_turn.credit_amount, 0)) AS balance_out,
                    COALESCE(pdr.balance_out_rub, 0) - SUM(COALESCE(dm_acc_turn.debet_amount_rub, 0))
                        + SUM(COALESCE(dm_acc_turn.credit_amount_rub, 0)) AS balance_out_rub
                FROM current_accounts ca
                LEFT JOIN prev_day_remains pdr
                    ON ca.account_rk = pdr.account_rk
                LEFT JOIN dm.dm_account_turnover_f dm_acc_turn
                    ON ca.account_rk = dm_acc_turn.account_rk
                        AND dm_acc_turn.on_date = i_onDate
                WHERE char_type = 'П'--русский символ
                GROUP BY ca.account_rk, pdr.balance_out, pdr.balance_out_rub
            )
            SELECT
                i_onDate,
                account_rk,
                balance_out,
                balance_out_rub
            FROM (
                SELECT * FROM active_accounts
                UNION ALL
                SELECT * FROM passive_accounts
                );
        --логирование
        INSERT INTO logs.proc_calc_logs (time_point, specification)
            VALUES (NOW(), 'Расчет account_balance за ' || i_onDate || '. Строк добавлено: ' ||
                (SELECT COUNT(*) FROM dm.dm_account_balance_f WHERE on_date = i_onDate));
    END;
$$ LANGUAGE plpgsql