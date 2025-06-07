import json
import duckdb
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--folder', type=str, help='queries folder', default='queries/')
args = parser.parse_args()


dbname = f'tpch_{args.sf}.db'
con = duckdb.connect(config={'allow_unsigned_extensions' : 'true'})
con.execute("CALL dbgen(sf="+str(args.sf)+");")
con.execute("PRAGMA threads=1")

print(con.execute("LOAD 'build/release/repository/v1.2.2/linux_amd64/fade.duckdb_extension'").df())
qid_list = [1, 3, 5, 6, 7, 8, 9, 10, 11, 12, 19]
for qid in qid_list:
    print(f"Eval: ------ {qid} ------")
    qfile = f"{args.folder}q{str(qid).zfill(2)}.sql"
    text_file = open(qfile, "r")
    query = text_file.read().strip()
    query = ' '.join(query.split())
    text_file.close()

    con.execute("PRAGMA set_lineage(true);").df()
    print(query)
    df = con.execute(query).df()
    con.execute("PRAGMA set_lineage(false);").df()
    print(df)

    con.execute("PRAGMA prepare_lineage(0)").df()
    #print(con.execute("select * from lineage_scan(0, 0)").df())

    print("------------")

    print(con.execute(f"PRAGMA whatif(0, 0, [], ['lineitem.l_returnflag','lineitem.l_linestatus'])").df())

    print(con.execute(f"select * from fade_reader(0)").df())

    con.execute("PRAGMA clear_lineage;")
