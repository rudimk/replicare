-- Source schema + seed data (MySQL 5.7). replicare installs its capture triggers
-- on this table. MySQL has no generate_series, so 100 rows come from a digit
-- cross-join (portable back to 5.7).
USE app;

CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR(100)) ENGINE=InnoDB;

INSERT INTO orders (id, note)
SELECT n, CONCAT('order ', n)
FROM (
  SELECT a.i + b.i * 10 + 1 AS n
  FROM (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
  CROSS JOIN (SELECT 0 i UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
              UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
) nums
WHERE n <= 100
ORDER BY n;
