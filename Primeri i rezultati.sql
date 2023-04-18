CREATE TABLE sales (
    id int,
    product text,
    amount int,
    price int,
    sale_date date,
    store_id int
) PARTITION BY RANGE (sale_date);


CREATE TABLE sales_2023_jan PARTITION OF sales FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE sales_2022_jan PARTITION OF sales FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
CREATE TABLE sales_2022_feb PARTITION OF sales FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
CREATE TABLE sales_2022_mar PARTITION OF sales FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');
CREATE TABLE sales_2022_apr PARTITION OF sales FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');
CREATE TABLE sales_2022_may PARTITION OF sales FOR VALUES FROM ('2022-05-01') TO ('2022-06-01');
CREATE TABLE sales_2022_jun PARTITION OF sales FOR VALUES FROM ('2022-06-01') TO ('2022-07-01');
CREATE TABLE sales_2022_jul PARTITION OF sales FOR VALUES FROM ('2022-07-01') TO ('2022-08-01');
CREATE TABLE sales_2022_aug PARTITION OF sales FOR VALUES FROM ('2022-08-01') TO ('2022-09-01');
CREATE TABLE sales_2022_sep PARTITION OF sales FOR VALUES FROM ('2022-09-01') TO ('2022-10-01');
CREATE TABLE sales_2022_oct PARTITION OF sales FOR VALUES FROM ('2022-10-01') TO ('2022-11-01');
CREATE TABLE sales_2022_nov PARTITION OF sales FOR VALUES FROM ('2022-11-01') TO ('2022-12-01');
CREATE TABLE sales_2022_dec PARTITION OF sales FOR VALUES FROM ('2022-12-01') TO ('2023-01-01');

INSERT INTO sales VALUES (1,'Some product',1,132,'2023-01-16',1);

PARTITION BY LIST (product);

CREATE TABLE our_product_sales PARTITION OF sales FOR VALUES IN ('Our product 1', 'Our product 2', 'Our product 3');

PARTITION BY HASH (store_id);

CREATE TABLE store_1_sales PARTITION OF sales FOR VALUES WITH (MODULUS 5, REMAINDER 1);

CREATE TABLE sales_2023_jan OF sales FOR VALUES FROM ('2023-01-01') TO ('2023-02-01') PARTITION BY LIST (product);

DROP TABLE sales_2022_jan, sales_2022_feb, ... sales_2022_dec;

ALTER TABLE sales DETACH PARTITION sales_2022_jan;

CREATE TABLE sales_2023_may (LIKE sales INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

ALTER TABLE sales_2023_may ADD CONSTRAINT 2023_may
   CHECK ( sale_date >= DATE '2023-05-01' AND sale_date < DATE '2023-06-01' );

ALTER TABLE sales ATTACH PARTITION sales_2023_may
FOR VALUES FROM ('2023-05-01') TO ('2023-06-01' );

CREATE INDEX sales_store_id_idx ON ONLY sales(store_id);

CREATE INDEX CONCURRENTLY sales_store_id_2023_may_idx ON sales_2023_may (store_id);
ALTER INDEX sales_store_id_idx ATTACH PARTITION sales_store_id_2023_may_idx;


SET enable_partition_pruning = on;
EXPLAIN SELECT COUNT(*) FROM sales WHERE sales.sale_date >= DATE '2022-12-01';

"Aggregate  (cost=50.60..50.61 rows=1 width=8)"
"  ->  Append  (cost=0.00..48.90 rows=680 width=0)"
"        ->  Seq Scan on sales_2022_dec sales_1  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2023_jan sales_2  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"

SET enable_partition_pruning = off;
EXPLAIN SELECT COUNT(*) FROM sales WHERE sales.sale_date >= DATE '2022-12-01';
"Aggregate  (cost=328.90..328.91 rows=1 width=8)"
"  ->  Append  (cost=0.00..317.85 rows=4420 width=0)"
"        ->  Seq Scan on sales_2022_jan sales_1  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_feb sales_2  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_mar sales_3  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_apr sales_4  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_may sales_5  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_jun sales_6  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_jul sales_7  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_aug sales_8  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_sep sales_9  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_oct sales_10  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_nov sales_11  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2022_dec sales_12  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"
"        ->  Seq Scan on sales_2023_jan sales_13  (cost=0.00..22.75 rows=340 width=0)"
"              Filter: (sale_date >= '2022-12-01'::date)"

CREATE TABLE sales_2023_may( CHECK ( sale_date >= DATE '2023-05-01' AND sale_date < DATE '2023-06-01' ) ) INHERITS(sales); 

CREATE OR REPLACE FUNCTION sales_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF ( NEW.sale_date >= DATE '2023-05-01' >= DATE '2006-02-01' AND
         NEW.sale_date < DATE '2023-06-01' ) THEN
        INSERT INTO sales_2023_may VALUES (NEW.*);
    ELSIF
    ...
    ELSE
        RAISE EXCEPTION 'Date out of range.  Update the trigger function sales_insert_trigger!';
    END IF;
    RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_sales_trigger
    BEFORE INSERT ON sales
    FOR EACH ROW EXECUTE FUNCTION sales_insert_trigger();

CREATE RULE sales_insert_2023_may AS
ON INSERT TO sales WHERE
    ( sale_date >= DATE '2023-05-01' AND sale_date < DATE '2023-06-01' )
DO INSTEAD
    INSERT INTO sales_2023_may VALUES (NEW.*);
...
CREATE RULE measurement_insert_y2008m01 AS
ON INSERT TO measurement WHERE
    ( logdate >= DATE '2008-01-01' AND logdate < DATE '2008-02-01' )
DO INSTEAD
    INSERT INTO measurement_y2008m01 VALUES (NEW.*);

CREATE SCHEMA partman;
CREATE EXTENSION pg_partman SCHEMA partman;

CREATE ROLE partman WITH LOGIN;
GRANT ALL ON SCHEMA partman TO partman;
GRANT ALL ON ALL TABLES IN SCHEMA partman TO partman;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA partman TO partman;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA partman TO partman;  -- PG11+ only
GRANT ALL ON SCHEMA my_partition_schema TO partman;
GRANT TEMPORARY ON DATABASE mydb to partman; -- allow creation of temp tables to move data out of default 

SELECT partman.create_parent( p_parent_table => 'sales',
 p_control => 'sale_date',
 p_type => 'native',
 p_interval=> 'monthly',
 p_premake => 12);

UPDATE partman.part_config 
SET infinite_time_partitions = true,
    retention = '3 months', 
    retention_keep_table=true 
WHERE parent_table = 'data_mart.events';

INSERT INTO sales VALUES (generate_series(1, 10000000),
						  'Product ' || trunc(random()*1000),
						  trunc(random()*10 +1),trunc(random()*100 + random()*10),
						 	timestamp '2022-01-01 20:00:00' +
       						random() * (timestamp '2023-01-20 20:00:00' -
                   			timestamp '2022-01-01 10:00:00'),
						  random()*5);


---------------------------------------------------------------------------
--Rezultati----------------------------------------------------------------
---------------------------------------------------------------------------

select * from sales_big where sale_date >= '2022-02-03' and sale_date <= '2022-04-06';
Query complete 00:00:01.734 -- upit bez particija

select * from sales where sale_date >= '2022-02-03' and sale_date <= '2022-04-06';
Query complete 00:00:00.998 -- upit sa particijama

-- Insert 100000000 podataka

INSERT 0 100000000

Query returned successfully in 1 min 13 secs. -- bez particija

INSERT 0 100000000

Query returned successfully in 1 min 24 secs. -- sa particijama




select * from sales_big where sale_date >= '2022-02-03' and sale_date <= '2022-06-06';

Query complete 00:00:28.734

select * from sales where sale_date >= '2022-02-03' and sale_date <= '2022-06-06';
Query complete 00:00:20.729

select * from sales_big where sale_date >= '2022-02-03' and sale_date <= '2022-02-16';
Query complete 00:00:03.284

select * from sales where sale_date >= '2022-02-03' and sale_date <= '2022-02-16';
Query complete 00:00:02.482

-- bez particija
"Gather  (cost=1000.00..1901715.40 rows=4043910 width=31) (actual time=24.106..1889.424 rows=4012467 loops=1)"
"  Workers Planned: 2"
"  Workers Launched: 2"
"  ->  Parallel Seq Scan on sales_big  (cost=0.00..1496324.40 rows=1684962 width=31) (actual time=56.927..1801.581 rows=1337489 loops=3)"
"        Filter: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-02-16'::date))"
"        Rows Removed by Filter: 35329178"
"Planning Time: 0.060 ms"
"JIT:"
"  Functions: 6"
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
"  Timing: Generation 0.966 ms, Inlining 94.868 ms, Optimization 49.596 ms, Emission 26.002 ms, Total 171.432 ms"
"Execution Time: 2003.729 ms"
-- sa particijama
"Bitmap Heap Scan on sales_2022_feb sales  (cost=54650.65..173670.12 rows=4007631 width=31) (actual time=87.417..501.921 rows=4004051 loops=1)"
"  Recheck Cond: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-02-16'::date))"
"  Heap Blocks: exact=58905"
"  ->  Bitmap Index Scan on sales_2022_feb_sale_date_idx  (cost=0.00..53648.74 rows=4007631 width=0) (actual time=80.150..80.150 rows=4004051 loops=1)"
"        Index Cond: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-02-16'::date))"
"Planning Time: 0.089 ms"
"JIT:"
"  Functions: 2"
"  Options: Inlining false, Optimization false, Expressions true, Deforming true"
"  Timing: Generation 0.324 ms, Inlining 0.000 ms, Optimization 0.000 ms, Emission 0.000 ms, Total 0.324 ms"
"Execution Time: 601.270 ms"

-- bez particija
"Seq Scan on sales_big  (cost=0.00..2458824.96 rows=29383213 width=31) (actual time=21.606..5680.326 rows=29479624 loops=1)"
"  Filter: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-05-16'::date))"
"  Rows Removed by Filter: 80520376"
"Planning Time: 0.058 ms"
"JIT:"
"  Functions: 2"
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
"  Timing: Generation 0.320 ms, Inlining 1.796 ms, Optimization 12.239 ms, Emission 7.413 ms, Total 21.769 ms"
"Execution Time: 6411.602 ms"
-- sa particijama
"Append  (cost=0.00..915227.81 rows=29536573 width=31) (actual time=71.451..3673.099 rows=29474189 loops=1)"
"  ->  Seq Scan on sales_2022_feb sales_1  (cost=0.00..179069.85 rows=7461703 width=31) (actual time=71.450..579.726 rows=7439964 loops=1)"
"        Filter: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-05-16'::date))"
"        Rows Removed by Filter: 571026"
"  ->  Seq Scan on sales_2022_mar sales_2  (cost=0.00..198386.71 rows=8875181 width=31) (actual time=0.148..588.165 rows=8875181 loops=1)"
"        Filter: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-05-16'::date))"
"  ->  Seq Scan on sales_2022_apr sales_3  (cost=0.00..191835.26 rows=8582084 width=31) (actual time=0.040..568.644 rows=8582084 loops=1)"
"        Filter: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-05-16'::date))"
"  ->  Seq Scan on sales_2022_may sales_4  (cost=0.00..198253.12 rows=4617605 width=31) (actual time=0.051..540.226 rows=4576960 loops=1)"
"        Filter: ((sale_date >= '2022-02-03'::date) AND (sale_date <= '2022-05-16'::date))"
"        Rows Removed by Filter: 4292248"
"Planning Time: 0.129 ms"
"JIT:"
"  Functions: 8"
"  Options: Inlining true, Optimization true, Expressions true, Deforming true"
"  Timing: Generation 0.914 ms, Inlining 1.853 ms, Optimization 41.528 ms, Emission 27.897 ms, Total 72.193 ms"
"Execution Time: 4406.372 ms"

