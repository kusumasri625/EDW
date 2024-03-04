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


def call_python_script(script_path, args):
    try:
        subprocess.run(["python", script_path] + args, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error calling {script_path}: {e}")


scripts_to_call = ['offices.py','employees.py','customers.py','payments.py','orders.py','productlines.py','products.py','orderdetails.py']


common_arguments = [str(etl_batch_date),str(etl_batch_no)]


def run_script(script):
    call_python_script(script, common_arguments)

if __name__ == '__main__':
    for script in scripts_to_call:
        run_script(script)
