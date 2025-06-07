import duckdb
import pandas as pd

with duckdb.connect("data/intel.db") as con:
    con.execute("drop table if exists readings")
    create_sql = """CREATE TABLE readings as SELECT * FROM 'data/intel.csv'"""
    con.execute(create_sql)

    print(con.execute("select * from readings").df())

    # hack since we don't have guards for null values
    con.execute("""UPDATE readings SET temp = COALESCE(temp, 0);""")
    con.execute("""UPDATE readings SET light = COALESCE(light, 0);""")
    con.execute("""UPDATE readings SET voltage = COALESCE(voltage, 0);""")
    con.execute("""UPDATE readings SET humidity = COALESCE(humidity, 0);""")

    con.execute("""UPDATE readings SET moteid = COALESCE(moteid, -1);""")
    print(con.execute("select * from readings").df())
