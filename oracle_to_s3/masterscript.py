import concurrent.futures
import sys
import subprocess
import os
import oracledb
sys.path.append('C:/Users/kusumasri.muddasani/Desktop/ETL/EDW')
from etlbatch import etl_batch


etl_batch_no = etl_batch()[0]
etl_batch_date = etl_batch()[1]
schema_name = etl_batch()[2]
identified = etl_batch()[3]


def connection(schema_name,identified):
    connection = oracledb.connect('tpc1gb/tpc1gb@3.234.208.164:1521/XE')
    cursor = connection.cursor()
    
    cursor.execute(f'Drop public database link f23kusumasri_dblink_classicmodels')
    query= f"CREATE PUBLIC database link f23kusumasri_dblink_classicmodels CONNECT TO {schema_name} IDENTIFIED BY {identified} USING 'XE'"

    cursor.execute(query)

    connection.close

def call_python_script(script_path, args):
    try:
        subprocess.run(["python", script_path] + args, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error calling {script_path}: {e}")


scripts_to_call = ['customers.py','products.py','employees.py','offices.py','orderdetails.py','orders.py','payments.py','productlines.py']


common_arguments = [str(etl_batch_date)]


def run_script(script):
    call_python_script(script, common_arguments)

if __name__ == '__main__':
    connection(schema_name,identified)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(run_script, scripts_to_call)