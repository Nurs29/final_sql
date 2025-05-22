# Создаем базу и загружаем данные из таблиц

CREATE DATABASE customers_transactions;

UPDATE customers
SET Gender = NULL
WHERE Gender = '';

UPDATE customers
SET Age = NULL
WHERE Age = '';

ALTER TABLE customers MODIFY Age INT NULL;

SELECT 
	*
FROM customers;

CREATE TABLE transactions
(
	date_new DATE,
    Id_check INT,
    ID_client INT,
    Count_products DECIMAL(10, 3),
    Sum_payment DECIMAL(10, 2)
);

LOAD DATA INFILE "C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\TRANSACTIONS_final.csv"
INTO TABLE transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SHOW VARIABLES LIKE 'secure_file_priv';

SELECT 
	*
FROM transactions;

/*
	Задание №1: Cписок клиентов с непрерывной историей за год, то есть каждый месяц на регулярной основе без пропусков за указанный годовой период, 
    средний чек за период с 01.06.2015 по 01.06.2016, средняя сумма покупок за месяц, количество всех операций по клиенту за период;
*/

SELECT * FROM transactions;

# Для начало создаем временную таблицу, так как результатом мы будем пользоваться в рамках текущего сеанса.
CREATE TEMPORARY TABLE temp_month AS    
SELECT
	ID_client,
    count(DISTINCT MONTH(date_new)) AS Months
FROM transactions
GROUP BY ID_client;

# Находим ID клиентов с непрерывной историей за год. 
SELECT 
	id_client 
FROM temp_month 
WHERE months = 12;

# Средний чек за период с 01.06.2015 по 01.06.2016
SELECT
	ID_client,
    AVG(Sum_payment) AS AvgPayment
FROM transactions
GROUP BY ID_client
HAVING ID_client IN (
					 SELECT 
					 id_client 
					 FROM temp_month 
					 WHERE months = 12
					 );

# Средняя сумма покупок за месяц
SELECT
	ID_client,
    date_new AS Date,
    MONTHNAME(date_new) AS Month,
    AVG(Sum_payment) AS AvgPaymentPerMonth
FROM transactions
GROUP BY date_new, ID_client
HAVING ID_client IN (
					 SELECT 
					 id_client 
					 FROM temp_month 
					 WHERE months = 12
					 )
ORDER BY ID_client;

# Количество всех операций по клиенту за период
SELECT
	ID_client,
    COUNT(Sum_payment) AS TransactionCount
FROM transactions
GROUP BY ID_client
HAVING ID_client IN (
					 SELECT 
					 id_client 
					 FROM temp_month 
					 WHERE months = 12
					 )
ORDER BY TransactionCount;


/*
	Задание №2: Информация в разрезе месяцев.
*/

# а) средняя сумма чека в месяц
SELECT
	ROUND(SUM(avg_check) / 12, 2) AS MonthlyAvgCheck
FROM (
    SELECT 
		MONTH(date_new) AS Month,
		AVG(sum_payment) AS avg_check
	FROM transactions
	GROUP BY Month
	ORDER BY Month
	)t;

# b) среднее количество операций в месяц
SELECT 
	ROUND(COUNT(id_check) / 12, 2)  AS MonthlyOperations
FROM transactions;

# c) среднее количество клиентов, которые совершали операции
SELECT
	AVG(Total_Clients) 
FROM (
	SELECT
		MONTH(date_new) AS Month,
		COUNT(DISTINCT ID_client) AS Total_Clients
	FROM transactions
	GROUP BY Month
    )t;
    
# d) долю от общего количества операций за год и долю в месяц от общей суммы операций, выполним задачу с помощью CTE
WITH CTE_Total AS 
(
SELECT
	COUNT(*) AS Total_Operations,
    SUM(Sum_payment) AS Total_Payments
FROM transactions
)
, CTE_Monthly AS
(
SELECT
	date_new,
	MONTH(date_new) AS Month,
    COUNT(*) AS Monthly_Operations,
    SUM(Sum_payment) AS Monthly_Payments
FROM transactions
GROUP BY date_new, Month
)
SELECT
	m.date_new,
    m.Monthly_Operations,
   ROUND(m.Monthly_Operations / t.Total_Operations * 100, 2) AS OperationPercentage,
    m.Monthly_Payments,
    ROUND(m.Monthly_Payments / t.Total_Payments * 100, 2) AS PaymentPercentage
FROM CTE_Monthly m
JOIN CTE_Total t
ORDER BY date_new;

# e) вывести % соотношение M/F/NA в каждом месяце с их долей затрат;    
SELECT
	t.date_new,
    c.gender,
    COUNT(DISTINCT t.ID_client) AS Gender_Clients_Count,
    SUM(COUNT(DISTINCT t.ID_client)) OVER (PARTITION BY t.date_new) AS Total_Clients_Count,
    ROUND(COUNT(DISTINCT t.ID_client) / SUM(COUNT(DISTINCT t.ID_client)) OVER (PARTITION BY t.date_new) * 100, 2) AS Gender_Ratio_Percent,
    SUM(t.Sum_payment) AS Payments_By_Gender,
    SUM(SUM(t.Sum_payment)) OVER(PARTITION BY t.date_new) AS MonthlyPayment,
    ROUND(SUM(t.Sum_payment) /  SUM(SUM(t.Sum_payment)) OVER(PARTITION BY t.date_new) * 100, 2) AS Payment_Ratio_By_Gender_Percent
FROM customers c
JOIN transactions t
ON c.Id_client =t.ID_client
GROUP BY t.date_new, c.Gender;


/*
	Задание №3: Возрастные группы клиентов с шагом 10 лет и отдельно клиентов, у которых нет данной информации, 
				с параметрами сумма и количество операций за весь период, и поквартально - средние показатели и %.
*/
WITH CTE_Group AS 
(
SELECT
	CASE
		WHEN Age BETWEEN 0 AND 10 THEN '00-10'
        WHEN Age BETWEEN 11 AND 20 THEN '11-20'
        WHEN Age BETWEEN 21 AND 30 THEN '21-30'
        WHEN Age BETWEEN 31 AND 40 THEN '31-40'
        WHEN Age BETWEEN 41 AND 50 THEN '41-50'
        WHEN Age BETWEEN 51 AND 60 THEN '51-60'
        WHEN Age BETWEEN 61 AND 70 THEN '61-70'
        WHEN Age BETWEEN 71 AND 80 THEN '71-80'
        WHEN Age BETWEEN 81 AND 90 THEN '81-90'
        WHEN Age IS NULL THEN 'N/A'
	END AS Age_Category,
    CONCAT(YEAR(t.date_new), ' - Q', quarter (t.date_new)) AS Quarter,
    SUM(t.sum_payment) AS Paypment_Per_Quarter,
    ROUND(AVG(t.Sum_payment), 2) AS Total_Avg_Payment
FROM customers c
JOIN transactions t
ON c.Id_client = t.ID_client
GROUP BY Age_Category, Quarter
ORDER BY Age_Category
)
, CTE_Overal_Payment AS
(
SELECT
	Quarter,
    SUM(Paypment_Per_Quarter) AS Total_Payment
FROM CTE_Group
GROUP BY Quarter
)
SELECT
	g.*,
    ROUND(g.Paypment_Per_Quarter / o.Total_Payment * 100, 2) AS Quarter_Percent
FROM CTE_Group g
JOIN CTE_Overal_Payment o
ON g.Quarter = o.Quarter;
