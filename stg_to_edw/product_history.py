import psycopg2
import sys
sys.path.append('C:/Users/kusumasri.muddasani/Desktop/ETL/EDW')
from etlbatch import etl_batch


etl_batch_no = etl_batch()[0]
etl_batch_date = etl_batch()[1]

# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"


def connect_to_redshift(host, port, database, user, password,ETL_BATCH_NO,ETL_BATCH_DATE):
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

        copy_command= (f'''UPDATE dev_edw.product_history c1
set effective_to_date='{ETL_BATCH_DATE}',
dw_active_record_ind=0,
dw_update_timestamp= current_timestamp
from dev_edw.products c
WHERE c1.dw_product_id = c.dw_product_id 
and c1.dw_active_record_ind=1 
and c1.msrp <> c.msrp;''')

        copy_command1=(f'''
insert into dev_edw.product_history(
dw_product_id,
MSRP,
effective_from_date,
dw_active_record_ind,
dw_create_timestamp,
dw_update_timestamp
)
SELECT c.dw_product_id,
c.MSRP,
'{ETL_BATCH_DATE}'effective_from_date,
1 dw_active_record_ind,
current_timestamp,
current_timestamp
FROM dev_edw.products c LEFT JOIN dev_edw.product_history c1
  ON c1.dw_product_id = c.dw_product_id and c1.dw_active_record_ind=1
WHERE c1.dw_product_id IS NULL
''')

        cursor.execute(copy_command)
        connection.commit()
        cursor.execute(copy_command1)
        connection.commit()

        print(f"Data uploaded to Redshift table product_history")

    except Exception as e:
        print(f"Error uploading to Redshift product_history: {e}")

    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

connect_to_redshift(
    redshift_host,
    redshift_port,
    redshift_database,
    redshift_user,
    redshift_password,
    etl_batch_no,
    etl_batch_date
)
