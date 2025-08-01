CREATE OR REPLACE PROCEDURE rd.fill_account_balance_turnover() AS $$
    BEGIN
        TRUNCATE TABLE dm.account_balance_turnover;
		INSERT INTO dm.account_balance_turnover
	        SELECT a.account_rk,
				COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
				a.department_rk,
				ab.effective_date,
				ab.account_in_sum,
				ab.account_out_sum
			FROM rd.account a
			LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
			LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
    END;
$$ LANGUAGE plpgsql
