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

        copy_command= (f'''INSERT INTO dev_edw.daily_customer_summary 
(SELECT summary_date,
       dw_customer_id,
       SUM(order_count) order_count,
       SUM(customer_unique_count) order_apd,
       SUM(ordered_amount) ordered_amount,
       SUM(order_cost_amount) order_cost_amount,
       SUM(order_mrp_amount) order_mrp_amount,
       SUM(cancelled_order_count) cancelled_order_count,
       SUM(cancelled_order_amount) cancelled_order_amount,
       SUM(cancelled_order_apd) cancelled_order_apd,
       SUM(shipped_order_count) shipped_order_count,
       SUM(shipped_order_amount) shipped_order_amount,
       SUM(shipped_order_apd) shipped_order_apd,
       SUM(payment_apd) payment_apd,
       SUM(payment_amount) payment_amount,
       SUM(new_customer_apd) new_customer_apd,
       SUM(new_customer_paid_apd) new_customer_paid_apd,
       CURRENT_TIMESTAMP as dw_create_timestamp,
       '{ETL_BATCH_NO}' etl_batch_no,
        '{ETL_BATCH_DATE}'  etl_batch_date
       
FROM (SELECT CAST(O.orderdate AS DATE) summary_date,
             O.dw_customer_id,
             COUNT(DISTINCT O.src_ordernumber) order_count,
             1 customer_unique_count,
             SUM(OD.quantityordered*OD.priceeach) ordered_amount,
             SUM(OD.quantityordered*P.buyprice) order_cost_amount,
             SUM(P.msrp*OD.quantityordered) order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
      FROM dev_edw.orders O
        JOIN dev_edw.orderdetails OD ON O.dw_order_id= OD.dw_order_id
        JOIN dev_edw.products P ON OD.dw_product_id = P.dw_product_id
      WHERE CAST(O.orderdate AS DATE)>= '{ETL_BATCH_DATE}' 
      GROUP BY CAST(O.orderdate AS DATE),
               O.dw_customer_id
      UNION ALL
      SELECT CAST(O.cancelleddate AS DATE) summary_date,
             O.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amount,
             0 order_cost_amount,
             0 order_mrp_amount,
             COUNT( distinct O.src_ordernumber) cancelled_order_count,
             SUM(OD.quantityordered*OD.priceeach) cancelled_order_amount,
             1 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
      FROM dev_edw.orders O
        JOIN dev_edw.orderdetails OD ON  O.dw_order_id= OD.dw_order_id
      WHERE CAST(O.cancelleddate AS DATE)>= '{ETL_BATCH_DATE}' 
      AND   O.status = 'Cancelled'
      GROUP BY CAST(O.cancelleddate AS DATE),
               O.dw_customer_id
      UNION ALL
      SELECT CAST(O.shippeddate AS DATE) summary_date,
             O.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amount,
             0 order_cost_amount,
             0 order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             COUNT(DISTINCT O.src_ordernumber) shipped_order_count,
             SUM(OD.quantityordered*OD.priceeach) shipped_order_amount,
             1 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
      FROM dev_edw.orders O
        JOIN dev_edw.orderdetails OD ON  O.dw_order_id= OD.dw_order_id
      WHERE CAST(O.shippeddate AS DATE)>= '{ETL_BATCH_DATE}' 
      AND   O.status = 'Shipped'
      GROUP BY CAST(O.shippeddate AS DATE),
               O.dw_customer_id
      UNION ALL
      SELECT CAST(P.paymentdate AS DATE) summary_date,
             P.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amount,
             0 order_cost_amount,
             0 order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             1 payment_apd,
             SUM(P.amount) payment_amount,
             0 new_customer_apd,
             0 new_customer_paid_apd
      FROM dev_edw.payments P
      WHERE CAST(P.paymentdate AS DATE)>='{ETL_BATCH_DATE}'
      GROUP BY CAST(P.paymentdate AS DATE),
               P.dw_customer_id
      UNION ALL
      SELECT CAST(c.src_create_timestamp AS DATE) summary_date,
             c.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amount,
             0 order_cost_amount,
             0 order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             1 new_customer_apd,
             0 new_customer_paid_apd
      FROM dev_edw.customers c
      WHERE CAST(c.src_create_timestamp AS DATE)>= '{ETL_BATCH_DATE}' 
      GROUP BY CAST(c.src_create_timestamp AS DATE),
               c.dw_customer_id
UNION ALL
      SELECT min(CAST(o.orderdate AS DATE)) summary_date,
             o.dw_customer_id,
             0 order_count,
             0 customer_unique_count,
             0 ordered_amount,
             0 order_cost_amount,
             0 order_mrp_amount,
             0 cancelled_order_count,
             0 cancelled_order_amount,
             0 cancelled_order_apd,
             0 shipped_order_count,
             0 shipped_order_amount,
             0 shipped_order_apd,
             0 payment_apd,
             0 payment_amount,
             0 new_customer_apd,
             1 new_customer_paid_apd
      FROM dev_edw.orders o
          GROUP BY o.dw_customer_id
          HAVING summary_date >= '{ETL_BATCH_DATE}')DCS
GROUP BY summary_date,
         dw_customer_id);
''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table daily_customer_summary")

    except Exception as e:
        print(f"Error uploading to Redshift daily_customer_summary: {e}")

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
