-- Target schema (MySQL 8.4). replicare replicates DATA ONLY (CLAUDE.md §7): the
-- target table must pre-exist, created with your own tooling. It starts empty;
-- replicare's initial copy fills it, then streaming keeps it converged. The
-- primary key must match the source's replication key.
--
-- In MySQL a schema IS a database, and replicare matches tables by their
-- `db.table` identity across servers (it never renames — that would be a
-- transform, CLAUDE.md §1.7). So the target database + table name must MATCH the
-- source: `app.orders` here too, not a renamed `warehouse`.
CREATE DATABASE IF NOT EXISTS app;
USE app;

CREATE TABLE orders (id INT PRIMARY KEY, note VARCHAR(100)) ENGINE=InnoDB;
