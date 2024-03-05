import concurrent.futures
import subprocess
import psycopg2

# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"

def upload_to_redshift(host, port, database, user, password):
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

        copy_command= ('''truncate table dev.dev_stg.customers;
                        truncate table dev.dev_stg.employees;
                        truncate table dev.dev_stg.offices;
                        truncate table dev.dev_stg.orderdetails;
                        truncate table dev.dev_stg.orders;
                        truncate table dev.dev_stg.payments;
                        truncate table dev.dev_stg.productlines;
                        truncate table dev.dev_stg.products;''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"truncated dev_stg tables")

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

scripts_to_call = ['customers.py','products.py','employees.py','offices.py','orderdetails.py','orders.py','payments.py','productlines.py']


def run_script(script):
    call_python_script(script)

if __name__ == '__main__':
    
    upload_to_redshift(
    redshift_host,
    redshift_port,
    redshift_database,
    redshift_user,
    redshift_password
    )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(run_script, scripts_to_call)
