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
   DELETE FROM dev_edw.monthly_product_summary
   WHERE start_of_the_month_date>=
     date_trunc('month', '{ETL_BATCH_DATE}'::date);
   """
        copy_command1 = f"""
   INSERT INTO dev_edw.monthly_product_summary
    (SELECT date_trunc('month', summary_date::date) start_of_the_date,
       dw_product_id,
       SUM(customer_apd) customer_apd,
       (CASE WHEN SUM(customer_apd) > 0 THEN 1 ELSE 0 END) customer_apm,
       SUM(product_cost_amount) product_cost_amount,
       SUM(product_mrp_amount) product_mrp_amount,
       SUM(cancelled_product_qty) cancelled_product_qty,
       SUM(cancelled_cost_amount) cancelled_cost_amount,
       SUM(cancelled_mrp_amount) cancelled_mrp_amount,
       SUM(cancelled_order_apd) cancelled_order_apd,
       (CASE WHEN SUM(cancelled_order_apd) > 0 THEN 1 ELSE 0 END)
       cancelled_order_apm,
       current_timestamp dw_create_timestamp,
       '{ETL_BATCH_NO}',
       '{ETL_BATCH_DATE}'
    FROM dev_edw.daily_product_summary
    WHERE date_trunc('month', summary_date::date)>=
      date_trunc('month', '{ETL_BATCH_DATE}'::date)
    GROUP BY date_trunc('month', summary_date::date),
         dw_product_id);
    """

        cursor.execute(copy_command)
        connection.commit()
        cursor.execute(copy_command1)
        connection.commit()

        print(f"Data uploaded to Redshift table monthly_product_summary")

    except Exception as e:
        print(f"Error uploading to Redshift monthly_product_summary: {e}")

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
