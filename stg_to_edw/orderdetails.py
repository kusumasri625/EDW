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

        copy_command= (f'''insert into dev_edw.orderdetails
(
 dw_order_id           ,
   dw_product_id        ,
   src_orderNumber      ,
   src_productCode      ,
   quantityOrdered      ,
   priceEach            ,
   orderLineNumber       ,
   src_create_timestamp  ,
   src_update_timestamp ,
   etl_batch_no,
   etl_batch_date 
)
SELECT
  o.dw_order_id
, p.dw_product_id
, od.ORDERNUMBER
, od.PRODUCTCODE
, od.QUANTITYORDERED
, od.PRICEEACH
, od.ORDERLINENUMBER
, od.create_timestamp
, od.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM dev_stg.orderdetails od left join dev_edw.orderdetails od1 on od.orderNumber=od1.src_orderNumber and od.productCode=od1.src_productCode 
inner join dev_edw.orders o on o.src_ordernumber=od.ordernumber
inner join dev_edw.products p on od.productcode=p.src_productcode
where od1.src_orderNumber is null and od1.src_productCode is null;



update dev_edw.orderdetails a
set
src_orderNumber      =b.ordernumber,
  src_productCode      =b.productcode,
   quantityOrdered      =b.quantityordered,
   priceEach            =b.priceeach,
   orderLineNumber       =b.orderlinenumber,
   src_update_timestamp=b.update_timestamp,
   dw_update_timestamp=current_timestamp,
   etl_batch_no          ='{ETL_BATCH_NO}' ,    
   etl_batch_date        ='{ETL_BATCH_DATE}'
   from dev_stg.orderdetails b
where a.src_ordernumber=b.ordernumber and a.src_productcode=b.productcode;
''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table orderdetails")

    except Exception as e:
        print(f"Error uploading to Redshift orderdetails: {e}")

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
