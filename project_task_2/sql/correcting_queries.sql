-- запрос из п.1, исправляющий входной остаток текущего дня
SELECT
	account_rk,
	effective_date,
	CASE
		WHEN account_in_sum = LAG(account_out_sum, 1) OVER w
			OR LAG(account_out_sum, 1) OVER w IS NULL
			THEN account_in_sum
		ELSE LAG(account_out_sum, 1) OVER w
	END AS corrected_account_in_sum,
	account_out_sum
FROM rd.account_balance
WINDOW w AS (PARTITION BY account_rk ORDER BY effective_date ASC)

-- запрос из п.2, исправляющий выходной остаток предыдущего дня
SELECT
	account_rk,
	effective_date,
	account_in_sum,
	CASE
		WHEN account_out_sum = LEAD(account_in_sum, 1) OVER w
			OR LEAD(account_in_sum, 1) OVER w IS NULL
			THEN account_out_sum
		ELSE LEAD(account_in_sum, 1) OVER w
	END AS corrected_account_out_sum
FROM rd.account_balance
WINDOW w AS (PARTITION BY account_rk ORDER BY effective_date ASC)