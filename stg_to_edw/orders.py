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

        copy_command= (f'''insert into dev_edw.orders
(
   dw_customer_id        ,
   src_orderNumber       ,
   orderDate             ,
   requiredDate          ,
   shippedDate           ,
   status                ,
   src_customerNumber    ,
   src_create_timestamp  ,
   src_update_timestamp  ,
   etl_batch_no          ,
   etl_batch_date        ,
   cancelleddate
)
SELECT
  c.dw_customer_id
, o.ORDERNUMBER
, o.ORDERDATE
, o.REQUIREDDATE
, o.SHIPPEDDATE
, o.STATUS
, o.CUSTOMERNUMBER
, o.create_timestamp
, o.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
, o.cancelleddate
FROM dev_stg.orders o left join dev_edw.orders o1 on o.orderNumber=o1.src_orderNumber
inner join dev_edw.customers c on o.customernumber=c.src_customernumber
where o1.src_orderNumber is null;

update dev_edw.orders a
set 
   orderDate=b.orderdate             ,
   requiredDate=b.requireddate          ,
   shippedDate=b.shippeddate           ,
   status=b.status                             ,
   src_customerNumber=b.customernumber    ,
   src_update_timestamp=b.update_timestamp,
   etl_batch_no ='{ETL_BATCH_NO}',
   etl_batch_date='{ETL_BATCH_DATE}',
   cancelleddate=b.cancelleddate,
   dw_update_timestamp=CURRENT_TIMESTAMP
   from dev_stg.orders b
where a.src_orderNumber=b.ordernumber;


''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table orders")

    except Exception as e:
        print(f"Error uploading to Redshift orders: {e}")

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
