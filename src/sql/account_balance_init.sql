--процедура для первичного заполнения dm.dm_account_balance_f данными за 31.12.2017
CREATE OR REPLACE PROCEDURE ds.init_account_balance_f() AS $$
    DECLARE
        i_onDate DATE;
    BEGIN
        i_onDate = '31-12-2017';
        DELETE FROM dm.dm_account_balance_f WHERE on_date = i_onDate;
        INSERT INTO dm.dm_account_balance_f
            SELECT
                i_onDate,
                ft_bal.account_rk AS account_rk,
                    --COALESCE(all_acc.account_rk, ft_bal.account_rk)
                ft_bal.balance_out AS balance_out,
                    --COALESCE(ft_bal.balance_out, 0)
                ft_bal.balance_out * COALESCE(ex_rat.reduced_cource, 1) AS balance_out_rub
                    --COALESCE(ft_bal.balance_out, 0) * COALESCE(ex_rat.reduced_cource, 1)
            FROM
            /*(
                SELECT DISTINCT credit_account_rk AS account_rk
                FROM ds.ft_posting_f
                UNION
                SELECT DISTINCT debet_account_rk AS account_rk
                FROM ds.ft_posting_f
            ) all_acc*/

            --для заполнения имеющихся остатков
                    ds.ft_balance_f ft_bal -- FULL JOIN ds.ft_balance_f ft_bal
                --ON all_acc.account_rk = ft_bal.account_rk
            
            --почему-то в ft_balance_f есть счета, которых нет в md_account_d: 31824186, 31483502;
            --  а в ft_posting_f операции со счетами,
            --  только часть из которых есть в ft_balance_f и md_account_d,
            --  причем ни один файл не содержит полного списка счетов...
            
            --легко путаться с таким количеством счетов, когда только для 112 из которых
            --  надо будет считать остатки в соответствующей витрине

            --всего получилось 1026 счетов из файлов ft_posting_f и ft_balance_f,
            --  но не буду отходить от задания и добавлю в таблицу только счета ft_balance_f

            --данные исследования и копания появились как результат осмысления 
            --  следующей фразы задания "берем остаток в валюте счета за предыдущий
            --  день (если его нет, то считаем его равным 0)", то есть предполагается, что
            --  могут быть счета, для которых остаток не определен, но тогда эти счета
            --  требовалось бы, наверное, брать откуда-то еще, кроме таблицы ds.md_account_d,
            --  для которой все остатки определены в ds.ft_balance_f, но такого действия не обозначается

            --для перевода курсов
            LEFT JOIN ds.md_exchange_rate_d ex_rat
                ON ft_bal.currency_rk = ex_rat.currency_rk
                    AND i_onDate BETWEEN ex_rat.data_actual_date AND ex_rat.data_actual_end_date
            ;
        --логирование
        INSERT INTO logs.proc_calc_logs (time_point, specification)
            VALUES (NOW(), 'Инициализация account_balance за ' || i_onDate || '. Строк добавлено: ' ||
                (SELECT COUNT(*) FROM dm.dm_account_balance_f WHERE on_date = i_onDate));
    END;
$$ LANGUAGE plpgsql