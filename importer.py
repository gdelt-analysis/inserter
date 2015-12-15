import os
import argparse
import json
import pymysql

# define args
parser = argparse.ArgumentParser(description="Download GDELT projects data and filter them.")
parser.add_argument('--host', type=str, default="localhost", help="MySQL host")
parser.add_argument('--user', type=str, default="root", help="MySQL user")
parser.add_argument('--passwd', type=str, required=True, help="MySQL user's password")
parser.add_argument('--db', type=str, required=True, help="MySQL database name")
parser.add_argument('--file', type=str, required=True, help="include url to geo filtering")
args = parser.parse_args()

if not os.path.isfile(args.file): raise Exception("File for import doesn't exists.")

# load gdelt data format definition
data_format = {}
with open("gdelt_format.csv") as f:
    f.readline()  # ignore first line
    for line in f:
        name, id, sql_type = line.replace("\n", '').split(",")
        data_format[int(id)] = {
            'name': name,
            'type': sql_type
        }

# generate sql: create table
with open(args.file) as f:
    header = json.loads(f.readline())

    # generování sql
    sql = "DROP TABLE IF EXISTS gdelt; CREATE TABLE gdelt ( \n"

    for key in header:
        sql += "{} {}, ".format(data_format[key]['name'], data_format[key]['type'])

    sql = sql[:-2] + ") CHARACTER SET utf8;"
    print(sql)

# load data to table

connection = pymysql.connect(host=args.host,
                             user=args.user,
                             password=args.passwd,
                             db=args.db,
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)
cursor = connection.cursor()
cursor.execute(sql)
sql = "truncate gdelt; LOAD DATA INFILE '{}' INTO TABLE gdelt FIELDS TERMINATED BY ';' IGNORE 1 LINES;".format(os.path.abspath(args.file))
print("\n\n" + sql + "\n\n")
print("RUN COMMAND ABOVE FROM MYSQL CLI!!!")
