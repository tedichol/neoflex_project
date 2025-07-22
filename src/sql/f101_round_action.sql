CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f(IN i_onDate DATE) AS $$
    DECLARE
        from_date_param DATE;
        to_date_param DATE;
    BEGIN
    --7 счетов второго порядка имеют и рублевые, и валютные счета
        from_date_param = i_onDate - interval '1 month';
        to_date_param = i_onDate - interval '1 day';
        --очищаем записи
        DELETE FROM dm.dm_f101_round_f
            WHERE from_date = from_date_param AND to_date = to_date_param;
        --вставляем новые
        INSERT INTO dm.dm_f101_round_f
            --сначала по балансовым счетам 2-го порядка группировать, потом уже рублевые/нерублевые
            WITH in_balance AS (
                --входные остатки для отчетного периода(за день до)
                SELECT
                    --первые 5 цифр - счет второго порядка
                    LEFT(md_acc.account_number, 5) AS ledger_account,
                    --рублевые счета
                    COALESCE(SUM(acc_bal.balance_out_rub) FILTER (WHERE
                        md_acc.currency_code IN ('810', '643')), 0) AS balance_in_rub,
                    --валютные счета
                    COALESCE(SUM(acc_bal.balance_out_rub) FILTER (WHERE
                        md_acc.currency_code NOT IN ('810', '643')), 0) AS balance_in_val
                FROM ds.md_account_d md_acc
                LEFT JOIN dm.dm_account_balance_f acc_bal
                    ON md_acc.account_rk = acc_bal.account_rk
                        AND acc_bal.on_date = from_date_param - interval '1 day'
                --только действующие счета на период формирования отчета
                WHERE md_acc.data_actual_date <= from_date_param
                    AND md_acc.data_actual_end_date >= to_date_param
                GROUP BY ledger_account
            ), out_balance AS (
                --конечные остатки за отчетный период(в последний день)
                SELECT
                    --первые 5 цифр - счет второго порядка
                    LEFT(md_acc.account_number, 5) AS ledger_account,
                    --рублевые счета
                    COALESCE(SUM(acc_bal.balance_out_rub) FILTER (WHERE
                        md_acc.currency_code IN ('810', '643')), 0) AS balance_out_rub,
                    --валютные счета
                    COALESCE(SUM(acc_bal.balance_out_rub) FILTER (WHERE
                        md_acc.currency_code NOT IN ('810', '643')), 0) AS balance_out_val
                FROM ds.md_account_d md_acc
                LEFT JOIN dm.dm_account_balance_f acc_bal
                    ON md_acc.account_rk = acc_bal.account_rk
                        AND acc_bal.on_date = to_date_param
                --только действующие счета на период формирования отчета
                WHERE md_acc.data_actual_date <= from_date_param
                    AND md_acc.data_actual_end_date >= to_date_param
                GROUP BY ledger_account
            ), turn_deb AS (
                --суммарный дебетовый оборот
                SELECT
                    --первые 5 цифр - счет второго порядка
                    LEFT(md_acc.account_number, 5) AS ledger_account,
                    --рублевые счета
                    COALESCE(SUM(acc_tur.debet_amount_rub) FILTER (WHERE
                        md_acc.currency_code IN ('810', '643')), 0) AS turn_deb_rub,
                    --валютные счета
                    COALESCE(SUM(acc_tur.debet_amount_rub) FILTER (WHERE
                        md_acc.currency_code NOT IN ('810', '643')), 0) AS turn_deb_val
                FROM ds.md_account_d md_acc
                LEFT JOIN dm.dm_account_turnover_f acc_tur
                    ON md_acc.account_rk = acc_tur.account_rk
                --обороты на период формирования отчета
                WHERE ((from_date_param <= acc_tur.on_date AND acc_tur.on_date <= to_date_param)
                        OR acc_tur.on_date IS NULL)
                    AND (md_acc.data_actual_date <= from_date_param
                        AND md_acc.data_actual_end_date >= to_date_param)
                /*без IS NULL потеряется часть счетов 2-го порядка, т.к. для части, относящихся к ним 
                    счетов первого уровня, если можно так сказать, не определено оборотов
                    в соответствующей таблице, что исказит результаты*/
                GROUP BY ledger_account
            ), turn_cre AS (
                --суммарный кредитовый оборот
                SELECT
                    --первые 5 цифр - счет второго порядка
                    LEFT(md_acc.account_number, 5) AS ledger_account,
                    --рублевые счета
                    COALESCE(SUM(acc_tur.credit_amount_rub) FILTER (WHERE
                        md_acc.currency_code IN ('810', '643')), 0) AS turn_cre_rub,
                    --валютные счета
                    COALESCE(SUM(acc_tur.credit_amount_rub) FILTER (WHERE
                        md_acc.currency_code NOT IN ('810', '643')), 0) AS turn_cre_val
                FROM ds.md_account_d md_acc
                LEFT JOIN dm.dm_account_turnover_f acc_tur
                    ON md_acc.account_rk = acc_tur.account_rk
                --обороты на период формирования отчета
                WHERE ((from_date_param <= acc_tur.on_date AND acc_tur.on_date <= to_date_param)
                        OR acc_tur.on_date IS NULL)
                    AND (md_acc.data_actual_date <= from_date_param
                    AND md_acc.data_actual_end_date >= to_date_param)
                GROUP BY ledger_account
            )
            SELECT
                from_date_param AS from_date,
                to_date_param AS to_date,
                ds_led_acc.chapter AS chapter,
                ds_led_acc.ledger_account,
                ds_led_acc.characteristic AS characteristic,
                inbal.balance_in_rub AS balance_in_rub,
                inbal.balance_in_val AS balance_in_val,
                inbal.balance_in_rub + inbal.balance_in_val AS balance_in_total,
                tudeb.turn_deb_rub AS turn_deb_rub,
                tudeb.turn_deb_val AS turn_deb_val,
                tudeb.turn_deb_rub + tudeb.turn_deb_val AS turn_deb_total,
                tucre.turn_cre_rub AS turn_cre_rub,
                tucre.turn_cre_val AS turn_cre_val,
                tucre.turn_cre_rub + tucre.turn_cre_val AS turn_cre_total,
                oubal.balance_out_rub AS balance_out_rub,
                oubal.balance_out_val AS balance_out_val,
                oubal.balance_out_rub + oubal.balance_out_val AS balance_out_total
            FROM ds.md_ledger_account_s ds_led_acc
            INNER JOIN in_balance inbal
                ON ds_led_acc.ledger_account = CAST(inbal.ledger_account AS INTEGER)
            INNER JOIN out_balance oubal
                ON ds_led_acc.ledger_account = CAST(oubal.ledger_account AS INTEGER)
            INNER JOIN turn_deb tudeb
                ON ds_led_acc.ledger_account = CAST(tudeb.ledger_account AS INTEGER)
            INNER JOIN turn_cre tucre
                ON ds_led_acc.ledger_account = CAST(tucre.ledger_account AS INTEGER)
            --только действующие счета на период формирования отчета
            /*WHERE md_acc.data_actual_date <= from_date_param
                AND md_acc.data_actual_end_date >= to_date_param*/
            ;
        --логирование
        INSERT INTO logs.f101_proc_logs (time_point, specification)
            VALUES (NOW(), 'Расчет f101_round за ' || from_date_param || ' - ' || to_date_param ||
                '. Строк добавлено: ' || (SELECT COUNT(*) FROM dm.dm_f101_round_f
                WHERE from_date = from_date_param AND to_date = to_date_param) ||
                '. Всего счетов 2-го порядка: ' || (SELECT COUNT(DISTINCT LEFT(md_acc.account_number, 5))
                FROM ds.md_account_d md_acc) || '/' || (SELECT COUNT(DISTINCT ledger_account)
                FROM ds.md_ledger_account_s)); 
    END;
$$ LANGUAGE plpgsql