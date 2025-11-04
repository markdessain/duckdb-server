# pip install pyarrow
# pip install adbc-driver-flightsql
# pip install sqlalchemy
# pip install flightsql-dbapi
# pip install pandas

from pyarrow import flight

client = flight.FlightClient(location="grpc://localhost:32010", disable_server_verification=True)

table = client.do_get(flight.Ticket("SELECT 1 AS a".encode("utf-8"))).read_all()

print(table)

print("")
print("")

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi

with adbc_driver_flightsql.dbapi.connect("grpc://localhost:32010", autocommit=True) as conn:
    with conn.cursor() as cur:
        a = cur.execute("SELECT 1 AS a, 2 AS b")
        print(cur.fetchall())


print("")
print("")

# Works on mac, not in linux
# import flightsql.sqlalchemy
# from sqlalchemy import func, select, text
# from sqlalchemy.engine import create_engine
# from sqlalchemy.schema import MetaData, Table

# engine = create_engine("datafusion+flightsql://localhost:32010?insecure=true")

# with engine.connect() as connection:
#     result = connection.execute(text('SELECT 1 as a'))
#     print(result.cursor.description)

#     print(result.fetchall())
