import json
from itertools import cycle
import time
import pyodbc

# Connect to DataBase
print("Start connecting to db..")
connect = pyodbc.connect('Driver={SQL Server};'
                         'Server=BAHRAM\BAHRAM;'
                         'Database=db;'
                         'Trusted_Connection=yes;')
if connect:
    time.sleep(2)
    print("Connect successfully...")

cursor = connect.cursor()
# Fetch data as json
cursor.execute('SELECT [JSON] FROM [db].[dbo].[Table_01]')
result = cursor.fetchall()

column_list = []
value_list = []
for row in result:

    # Create a new json
    convertedDict = json.loads(str(row[0]))

    for k, v in convertedDict['product'][0].items():
        if k == "category":
            column_list.append(k)

        for item in v:
            if type(item) == dict:
                for key in item:
                    column_list.append(key)

    for index in convertedDict['product']:
        if isinstance(index, dict):
            for k, v in index.items():
                if type(v) == str:
                    value_list.append(v)
                if type(v[0]) == dict:
                    for value in v[0].values():
                        value_list.append(value)

data = list(zip(cycle(column_list), value_list))
result = [dict(data[i:i + len(column_list)]) for i in range(len(data))[::len(column_list)]]

time.sleep(2)
print("Start Inserting Data...")
for i in result:
    # cursor.execute(f'CREATE TABLE ProductOrder ( id int IDENTITY(1,1) PRIMARY KEY, {column[0]} nvarchar(50),{column[1]} nvarchar(50), {column[2]} nvarchar(50),{column[3]} int)')
    # connect.commit()
    cursor.execute(
        f"insert into ProductOrder("
        f"{column_list[0]}, "
        f"{column_list[1]}, "
        f"{column_list[2]}, "
        f"{column_list[3]}) "
        f"values(" \
        f"'{i['category']}'," \
        f"'{i['name']}'," \
        f"'{i['qty']}'," \
        f"'{i['price']}')"
    )
time.sleep(2)
print("Insert successfully.")
connect.commit()
connect.close()
time.sleep(2)
print("Connection closed.")
