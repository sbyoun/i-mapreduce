CREATE TABLE INPUT4(KEY STRING, VALUE STRING);
EXPLAIN
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE INPUT4;
LOAD DATA LOCAL INPATH '../data/files/kv1.txt' INTO TABLE INPUT4;
SELECT INPUT4.VALUE, INPUT4.KEY FROM INPUT4;
DROP TABLE INPUT4;