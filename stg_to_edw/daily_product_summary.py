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

        copy_command= (f'''insert into dev_edw.daily_product_summary
(select summary_date,
dw_product_id,
max(customer_apd) customer_apd,
max(product_cost_amount) product_cost_amount,
max(product_mrp_amount) product_mrp_amount,
max(cancelled_product_qty) cancelled_product_qty,
max(cancelled_cost_amount) cancelled_cost_amount,
max(cancelled_mrp_amount) cancelled_mrp_amount,
max(cancelled_order_apd)cancelled_order_apd,
CURRENT_TIMESTAMP dw_create_timestamp,
'{ETL_BATCH_NO}' etl_batch_no,
'{ETL_BATCH_DATE}' etl_batch_date
from (SELECT CAST(o.orderDate AS DATE) summary_date,
       p.dw_product_id,
       count(distinct o.src_customerNumber) customer_apd,
       sum(od.priceeach*od.quantityordered) product_cost_amount,
       sum(p.msrp*od.quantityordered) product_mrp_amount,
       0 cancelled_product_qty,
       0 cancelled_cost_amount,
       0 cancelled_mrp_amount,
       0 cancelled_order_apd
FROM dev_edw.products p
  INNER JOIN dev_edw.orderdetails od ON p.dw_product_id = od.dw_product_id
  JOIN dev_edw.orders o ON od.dw_order_id = o.dw_order_id
WHERE CAST(o.orderDate AS DATE) >='{ETL_BATCH_DATE}'
GROUP BY CAST(o.orderDate AS DATE),
         p.dw_product_id
union all
SELECT CAST(o.cancelledDate AS DATE) summary_date,
       p.dw_product_id,
       0 customer_apd,
       0 product_cost_amount,
       0 product_mrp_amount,
       count(distinct p.src_productcode) cancelled_product_qty,
       sum(od.priceeach*od.quantityordered) cancelled_cost_amount,
       sum(p.msrp*od.quantityordered) cancelled_mrp_amount,
       1 cancelled_order_apd
FROM dev_edw.products p
  INNER JOIN dev_edw.orderdetails od ON p.dw_product_id = od.dw_product_id
  JOIN dev_edw.orders o ON od.dw_order_id = o.dw_order_id
WHERE CAST(o.cancelledDate AS DATE) >='{ETL_BATCH_DATE}' and o.status='Cancelled'
GROUP BY CAST(o.cancelledDate AS DATE),
         p.dw_product_id)a
group by summary_date,dw_product_id)
''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table daily_product_summary")

    except Exception as e:
        print(f"Error uploading to Redshift daily_product_summary: {e}")

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
