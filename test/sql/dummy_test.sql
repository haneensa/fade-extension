--- description: test dummy operator basic functionality

statement ok
CREATE TABLE test(id INTEGER);

statement ok
INSERT INTO test VALUES (1), (2), (3);

query I
SELECT * FROM test;
----
1
2
3
