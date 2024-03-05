import psycopg2
from io import StringIO
import subprocess
import os

etl_batch_no=1001
etl_batch_date='2001-01-01'
schema_name = 'cm_20050609'
identified = 'cm_20050609123'

# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"

script_dir = r"C:\\Users\\kusumasri.muddasani\\Desktop\\ETL\\EDW"
scripts_to_call = [
os.path.join(script_dir, "oracle_to_s3/masterscript.py"),
os.path.join(script_dir, "src_to_stg/master.py"),
os.path.join(script_dir, "stg_to_edw/master.py"),
]
def etl_batch():
    connection = psycopg2.connect(
    host="default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com",
    port="5439",
    database="dev",
    user="admin",
    password="BizAct#12345"
    )
    cursor = connection.cursor()

    query1 = f'update dev.etl_metadata.batch_control set etl_batch_no={etl_batch_no}, etl_batch_date=\'{etl_batch_date}\';'
    cursor.execute(query1)

    query= f'select * from dev.etl_metadata.batch_control;'
    cursor.execute(query)
    result=cursor.fetchone()

    date_string=[]
    date_string.append(result[0])
    # Extract the date string
    s = result[1].strftime('%Y-%m-%d') if result and result[1] else None
    date_string.append(s)
    
    date_string.append(schema_name)
    
    date_string.append(identified)
    
    connection.commit()

    cursor.close()
    connection.close()

    return date_string

def etl_log_insert(host, port, database, user, password,etl_batch_no,etl_batch_date):
    try:
        # Create a connection to Redshift
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        copy_command= (f'''INSERT INTO dev.etl_metadata.batch_control_log
                         (
                            select
                            '{etl_batch_no}' etl_batch_no,
                            '{etl_batch_date}' etl_batch_date,
                            'R' etl_batch_status,
                            current_timestamp etl_batch_start_time,
                            null etl_batch_end_time
                            
                            )''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data inserted to Redshift table:batch_control_log")

    except Exception as e:
        print(f"Error uploading to Redshift: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

def etl_log_update(host, port, database, user, password,etl_batch_no,etl_batch_date):
    try:
        # Create a connection to Redshift
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()

        copy_command= (f'''update dev.etl_metadata.batch_control_log
                        set etl_batch_status='C',
                        etl_batch_end_time=current_timestamp
                        where etl_batch_no='{etl_batch_no}';''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data updated to Redshift table:batch_control_log")

    except Exception as e:
        print(f"Error uploading to Redshift: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()


def call_python_script(script_path):
    try:
        subprocess.run(["python", script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error calling {script_path}: {e}")


if __name__ == '__main__':
    
    etl_batch()
    
    etl_log_insert(redshift_host, redshift_port, redshift_database, redshift_user, redshift_password,etl_batch_no,etl_batch_date)

    # Run scripts sequentially
    for script in scripts_to_call:
        if os.path.exists(script):
            call_python_script(script)
        else:
            print(f"Script not found: {script}")

        
    etl_log_update(redshift_host, redshift_port, redshift_database, redshift_user, redshift_password,etl_batch_no,etl_batch_date)