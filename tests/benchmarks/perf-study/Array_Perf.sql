--
--   File: Array_Perf.sql
--
--  About: 
--
--    The purpose of this script is to test the efficiency of the PostgreSQL 
--    engine handling the kinds of queries we're interested in. 
--
--  CREATE ARRAY Test_Array <
--     int32_attr  : int32,
--     int64_attr  : int64,
--     double_attr : double
--  >
--  [ I=0:7999,1000,0, J=0:7999, 1000,0 ]
--
-- Hygiene. 
--
DROP TABLE Test_Array;
DROP TABLE Timings;
--
-- DDL 1: CREATE the Array/Table
--
CREATE TABLE Test_Array ( 
    I            BIGINT NOT NULL,
    J            BIGINT NOT NULL,
    int32_attr   INTEGER NOT NULL,
    int64_attr   BIGINT NOT NULL,
    double_attr  DOUBLE PRECISION NOT NULL,
        PRIMARY KEY ( I, J )
);
--
-- DDL 2: CREATE the timings table; 
--
CREATE TABLE Timings ( 
	Query_Num	INTEGER  NOT NULL PRIMARY KEY,
	Query_Time	TIMESTAMP NOT NULL
);
--
-- DDL 3:  Populate the array. 
--
INSERT INTO Test_Array 
( I, J, int32_attr, int64_attr, double_attr )
SELECT N1.Num + N2.Num * 10 + N3.Num * 100 + N4.Num * 1000 AS I,
       N5.Num + N6.Num * 10 + N7.Num * 100 + N8.Num * 1000 AS J,
       random() * 2000000000 AS int32_attr,
       random() *2000000000000000 AS int64_attr,
       random() AS double_attr
  FROM ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N1 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N2 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N3 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7)) AS N4 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N5 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N6 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N7 ( Num ),
       ( VALUES (0),(1),(2),(3),(4),(5),(6),(7)) AS N8 ( Num );
--
--  DML 0: Initialize the list of query timings. . . 
-- 
DELETE FROM Timings;
ANALYZE;
--
INSERT INTO Timings
( Query_Num, Query_Time ) 
VALUES 
( 0, now() );
--
--  DML 1: Grand aggregates. 
--
SELECT COUNT(*),
       sum ( int32_attr ),
       sum ( int64_attr ),
       sum ( double_attr )
  FROM Test_Array;
--
INSERT INTO Timings
( Query_Num, Query_Time ) 
VALUES 
( 1, now() );
--
--  DML 2: Aggregate with GROUP-BY I
--
WITH Q AS (SELECT COUNT(*), T.I FROM Test_Array T GROUP BY T.I)
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time ) 
VALUES 
( 2, now() );
--
--  DML 3: Aggregate with GROUP-BY J
--
WITH Q AS (SELECT COUNT(*), T.J FROM Test_Array T GROUP BY T.J)
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 3, now() );
--
--  DML 4: 64 x 1% query probes
--
WITH Q AS ( 
  SELECT S.ID, sum(T.int32_attr),count(*)
    FROM Test_Array T,
       ( VALUES 
  	( 1, 100,100,900,900), (2, 1100,100,1900,900), 
  	( 3, 100, 1100, 900, 1900), (4, 1100, 1100, 1900, 1900), 
  	( 5, 1100, 100, 1900, 900), (6, 3100, 100, 3900, 900), 
  	( 7 ,1100, 2100, 1900, 2900), (8, 3100, 2100, 3900, 2900),
  	( 9, 100, 1100, 900, 1900), (10, 1100, 1100, 1900, 1900),
  	(11, 100, 3100, 900, 3900), (12, 1100, 3100, 1900, 3900),
  	(13, 1100, 1100, 1900, 1900), (14, 3100, 1100, 3900, 1900),
  	(15, 1100, 3100, 1900, 3900), (16,100, 3100, 3900, 3900), 
  	(17, 1100, 100, 1900, 900), (18, 4100, 100, 4900, 900),
  	(19,1100, 1100, 1900, 1900), (20,4100, 1100, 4900, 1900),
  	(21, 3100, 100, 3900, 900), (22, 7100, 100, 7900, 900),
  	(23, 3100, 4100, 3900, 4900), (24, 7100, 4100, 7900, 4900),
  	(25, 1100, 2100, 1900, 2900), (26, 4100, 2100, 4900, 2900),
  	(27, 1100, 6100, 1900, 6900), (28, 4100, 6100, 4900, 6900),
  	(29, 3100, 2100, 3900, 2900), (30, 7100, 2100, 7900, 2900),
  	(31, 3100, 6100, 3900, 6900), (32, 7100, 6100, 7900, 6900), 
  	(32, 100, 1100, 900, 1900), (34, 1100, 1100, 1900, 1900),
  	(33, 100, 4100, 900, 4900), (36, 1100, 4100, 1900, 4900),
  	(35, 1100, 1100, 1900, 1900), (38, 3100, 1100, 3900, 1900),
  	(39, 1100, 5100, 1900, 5900), (40, 3100, 5100, 3900, 5900),
  	(41, 100, 3100, 900, 3900), (42, 1100, 3100, 1900, 3900),
  	(43, 100, 7100, 900, 7900), (44, 1100, 7100, 1900, 7900),
  	(45, 1100, 3100, 1900, 3900), (46, 3100, 3100, 3900, 3900),
  	(47, 1100, 7100, 1900, 7900), (48, 3100, 7100, 3900, 7900),
  	(49, 1100, 1100, 1900, 1900), (50, 4100, 1100, 4900, 1900),
  	(51, 1100, 4100, 1900, 4900), (52, 4100, 4100, 4900, 4900),
  	(53, 3100, 1100, 3900, 1900), (54, 7100, 1100, 7900, 1900), 
  	(55, 3100, 5100, 3900, 5900), (56, 7100, 5100, 7900, 5900),
  	(57, 1100, 3100, 1900, 3900), (58, 4100, 3100, 4900, 3900),
  	(59, 1100, 7100, 1900, 7900), (60, 4100, 7100, 4900, 7900),
  	(61, 3100, 3100, 3900, 3900), (62, 7100, 3100, 7900, 3900),
  	(63, 3100, 7100, 3900, 7900), (64, 7100, 7100, 7900, 7900)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 4, now() );
--
--   DML 5: 16 x 12.5% overlapping blocks   
--
WITH Q AS ( 
  SELECT S.ID, sum(T.int32_attr),count(*)
    FROM Test_Array T,
       ( VALUES 
  	( 1, -200, -200, 2200, 2200), (2, 1800, -200, 4200, 2200),
  	( 3, -200, 1800, 2200, 4200), (4, 1800, 1800, 4200, 4200),
  	( 5, 1800, -200, 4200, 2200), (6, 5800, -200, 8200, 2200),
  	( 7, 1800, 3800, 4200, 6200), (8, 5800, 3800, 8200, 6200),
  	( 9, -200, 1800, 2200, 4200), (10, 1800, 1800, 4200, 4200),
        (11, -200, 5800, 2200, 8200), (12,1800, 5800, 4200, 8200),
        (13, 1800, 1800, 4200, 4200), (14, 5800, 1800, 8200, 4200),
        (15, 1800, 5800, 4200, 8200), (16, 5800, 5800, 8200, 8200)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 5, now() );
--
--   DML 6: 4 x 25% overlapping blocks   
--
WITH Q AS (
  SELECT S.ID, sum(T.int32_attr), count(*)
    FROM Test_Array T,
       ( VALUES
        ( 1, 60, 60, 4060, 4060), (2, 3860, 3860, 7860, 7860),
        ( 3, 60, 3860, 4060, 7860), (4, 3860, 60, 7860, 4060)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 6, now() );
--
-- DML 7 : 4 x 25% overlaps with a 50% filter ... 
-- 
WITH Q AS (
  SELECT S.ID, sum(T.int32_attr), count(*)
    FROM Test_Array T,
       ( VALUES
        ( 1, 60, 60, 4060, 4060), (2, 3860, 3860, 7860, 7860),
        ( 3, 60, 3860, 4060, 7860), (4, 3860, 60, 7860, 4060)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
     AND T.double_attr > 0.5 
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 7, now() );
--
-- DML 8 : 4 x 25% overlaps with a 50% filter with a plus ... 
--
WITH Q AS (
  SELECT S.ID, sum(T.int64_attr + T.int32_attr), count(*)
    FROM Test_Array T,
       ( VALUES
        ( 1, 60, 60, 4060, 4060), (2, 3860, 3860, 7860, 7860),
        ( 3, 60, 3860, 4060, 7860), (4, 3860, 60, 7860, 4060)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
     AND T.double_attr > 0.5 
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 8, now() );
--
--  DML 9 : 2 x 25% overlaps with a filter and a plus, but index only 
-- 
WITH Q AS (
  SELECT S.ID, sum(T.I + T.J), count(*)
    FROM Test_Array T,
       ( VALUES
        ( 1, 60, 60, 4060, 4060), (2, 3860, 3860, 7860, 7860)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
     AND (T.I * 1000 + T.J) % 10 < 5
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES 
( 9, now() );
--
--  DML 10 : 2 x 25% overlaps with a complex filter and a plus over expression
--
WITH Q AS (
  SELECT S.ID, 
         avg(log((
		((2.0 * CAST (T.int32_attr AS double precision))+
		 (2.0 * CAST (T.int64_attr AS double precision)))
		*T.double_attr))),
	count(*) 
    FROM Test_Array T,
       ( VALUES        ( 1, 60, 60, 4060, 4060), (2, 3860, 3860, 7860, 7860)
       ) AS S ( ID, iMin, jMin, iMax, Jmax )
   WHERE T.I BETWEEN S.iMin AND S.iMax
     AND T.J BETWEEN S.jMin AND S.jMax
     AND ((T.double_attr + T.double_attr) / 2.0 ) < 0.5
 GROUP BY S.ID )
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES
( 10, now() );
--
-- DML 11: regrid equivalent. 
-- 
--  Tricky SQL, this. . . 
--
WITH Q AS ( 
   SELECT (N1.Num * 10 + N2.Num) AS ID, 
          avg ( T.int32_attr ), 
          avg ( T.int64_attr ), 
          avg ( T.double_attr )
     FROM Test_Array T,
          ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N1 ( Num ),
          ( VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9) ) AS N2 ( Num )
    WHERE T.I BETWEEN ((N1.Num * 10 + N2.Num) * 80) AND 
                      ((N1.Num * 10 + N2.Num) * 81) AND 
          T.J BETWEEN ((N1.Num * 10 + N2.Num) * 80) AND 
                      ((N1.Num * 10 + N2.Num) * 81)
  GROUP BY ID
) 
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES
( 11, now() );
--
--  DML 12: window() equivalent ... 
--
WITH Q AS (
   SELECT T1.I * 8000 + T1.J AS ID,
          avg ( T2.int32_attr ),
          avg ( T2.int64_attr ),
          avg ( T2.double_attr )
     FROM Test_Array T1, 
          Test_Array T2
    WHERE T2.I BETWEEN T1.I - 5 AND T1.I + 5
      AND T2.J BETWEEN T1.J - 5 AND T1.J + 5
  GROUP BY ID
)
SELECT COUNT(*) FROM Q;
--
INSERT INTO Timings
( Query_Num, Query_Time )
VALUES
( 12, now() );
--
--   DML 
--   Print the timing results per query. 
--
SELECT T2.Query_Num,
       EXTRACT ( 'epoch' FROM T2.Query_Time - T1.Query_Time)
  FROM Timings T1, Timings T2
 WHERE T2.Query_Num = T1.Query_Num + 1;

