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

        copy_command = f"""
   INSERT INTO dev_edw.product_history
   (
     dw_product_id,
     MSRP,
     effective_from_date,
     dw_active_record_ind,
     dw_create_timestamp,
     dw_update_timestamp,
     create_etl_batch_no,
     create_etl_batch_date
    )
    SELECT a.dw_product_id,
       a.MSRP,
       '{ETL_BATCH_DATE}' effective_from_date,
       1 dw_active_record_ind,
       CURRENT_TIMESTAMP,
       CURRENT_TIMESTAMP,
       '{ETL_BATCH_NO}',
       '{ETL_BATCH_DATE}'
    FROM dev_edw.products a
    LEFT JOIN dev_edw.product_history b
         ON a.dw_product_id = b.dw_product_id
         AND b.dw_active_record_ind = 1
    WHERE b.dw_product_id IS NULL;
"""
        copy_command1 = f"""
   UPDATE dev_edw.product_history
   SET effective_to_date ='{ETL_BATCH_DATE}',
       dw_active_record_ind = 0,
       dw_update_timestamp = CURRENT_TIMESTAMP,
       update_etl_batch_no ='{ETL_BATCH_NO}',
       update_etl_batch_date='{ETL_BATCH_DATE}'
  from dev_edw.products p inner join dev_edw.product_history
    ph on ph.dw_product_id=p.dw_product_id
  AND ph.dw_active_record_ind = 1
   WHERE ph.MSRP <> p.MSRP;
 """

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
