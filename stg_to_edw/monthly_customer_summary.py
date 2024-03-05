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

        copy_command= (f'''
DELETE FROM dev_edw.monthly_customer_summary 
WHERE start_of_the_month_date >= '{ETL_BATCH_DATE}'::date;

INSERT INTO dev_edw.monthly_customer_summary
SELECT 
    date_trunc('month', summary_date) AS start_of_the_month_date,
    dw_customer_id,
    SUM(order_count),
    SUM(order_apd),
    CASE WHEN SUM(order_apd) > 0 THEN 1 ELSE 0 END AS order_apm,
    SUM(ordered_amount),
    SUM(order_cost_amount),
    SUM(order_mrp_amount),
    SUM(cancelled_order_count),
    SUM(cancelled_order_amount),
    SUM(cancelled_order_apd),
    CASE WHEN SUM(cancelled_order_apd) > 0 THEN 1 ELSE 0 END AS cancelled_order_apm,
    SUM(shipped_order_count),
    SUM(shipped_order_amount),
    SUM(shipped_order_apd),
    CASE WHEN SUM(shipped_order_apd) > 0 THEN 1 ELSE 0 END AS shipped_order_apm,
    SUM(payment_apd),
    CASE WHEN SUM(payment_apd) > 0 THEN 1 ELSE 0 END AS payment_apm,
    SUM(payment_amount),
    SUM(new_customer_apd),
    CASE WHEN SUM(new_customer_apd) > 0 THEN 1 ELSE 0 END AS new_customer_apm,
    SUM(new_customer_paid_apd),
    CASE WHEN SUM(new_customer_paid_apd) > 0 THEN 1 ELSE 0 END AS new_customer_paid_apm,
    current_timestamp AS dw_create_timestamp,
    '{ETL_BATCH_NO}' AS etl_batch_no,
    '{ETL_BATCH_DATE}'::date AS etl_batch_date
FROM dev_edw.daily_customer_summary
WHERE date_trunc('month', summary_date) >= date_trunc('month', '{ETL_BATCH_DATE}'::date)
GROUP BY date_trunc('month', summary_date), dw_customer_id;
''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table monthly_customer_summary")

    except Exception as e:
        print(f"Error uploading to Redshift monthly_customer_summary: {e}")

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
