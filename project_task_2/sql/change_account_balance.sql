BEGIN;

CREATE TEMPORARY TABLE correct_acc_bal_table AS
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
	WINDOW w AS (PARTITION BY account_rk ORDER BY effective_date ASC);

TRUNCATE TABLE rd.account_balance;

INSERT INTO rd.account_balance
	SELECT *
	FROM correct_acc_bal_table;

DROP TABLE correct_acc_bal_table;

COMMIT;