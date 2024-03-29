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

        copy_command= (f'''update dev_edw.products a
set
 productName         =b.productname,
   productLine           =b.productline,
   productScale          =b.productscale,
   productVendor         =b.productvendor,
   quantityInStock       =b.quantityinstock,
   buyPrice              =b.buyprice,
   MSRP                  =b.msrp,
   src_update_timestamp  =b.update_timestamp,
   dw_update_timestamp   =current_timestamp,
   etl_batch_no          ='{ETL_BATCH_NO}',
   etl_batch_date        ='{ETL_BATCH_DATE}'
   from dev_stg.products b
where a.src_productcode=b.productcode ;''')

        copy_command1=(f'''
insert into dev_edw.products
(
src_productCode       ,
   productName         ,
   productLine           ,
   productScale          ,
   productVendor         ,
   quantityInStock       ,
   buyPrice              ,
   MSRP                  ,
   dw_product_line_id    ,
   src_create_timestamp  ,
   src_update_timestamp  ,
   etl_batch_no,
   etl_batch_date
   
)
SELECT
  p.PRODUCTCODE
, p.PRODUCTNAME
, p.PRODUCTLINE
, p.PRODUCTSCALE
, p.PRODUCTVENDOR
, p.QUANTITYINSTOCK
, p.BUYPRICE
, p.MSRP
, pl.dw_product_line_id
, p.create_timestamp
, p.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM dev_stg.products p left join dev_edw.products p1 on p.productCode=p1.src_productCode
inner join dev_edw.productlines pl on p.productline=pl.productline
where p1.src_productCode is null;
''')

        cursor.execute(copy_command1)
        connection.commit()
        cursor.execute(copy_command)
        connection.commit()

        print(f"Data uploaded to Redshift table products")

    except Exception as e:
        print(f"Error uploading to Redshift products: {e}")

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
