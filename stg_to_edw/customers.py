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

        copy_command= ('''INSERT INTO dev_edw.customers
(  src_customerNumber     ,
   customerName           ,
   contactLastName        ,
   contactFirstName       ,
   phone                  ,
   addressLine1           ,
   addressLine2            ,
   city                    ,
   state                   ,
   postalCode              ,
   country                ,
   salesRepEmployeeNumber  ,
   creditLimit             ,
   src_create_timestamp    ,
   src_update_timestamp ,
   etl_batch_no ,
   etl_batch_date  
)
SELECT
  c.CUSTOMERNUMBER
, c.CUSTOMERNAME
, c.CONTACTLASTNAME
, c.CONTACTFIRSTNAME
, c.PHONE
, c.ADDRESSLINE1
, c.ADDRESSLINE2
, c.CITY
, c.STATE
, c.POSTALCODE
, c.COUNTRY
, c.SALESREPEMPLOYEENUMBER
, c.CREDITLIMIT
, c.create_timestamp
, c.update_timestamp
, '${ETL_BATCH_NO}'
, '${ETL_BATCH_DATE}'
FROM dev_stg.customers c left join dev_edw.customers c1 on c.customerNumber=c1.src_customerNumber
left join dev_edw.employees e on c.salesrepemployeenumber=e.employeenumber
where c1.src_customerNumber is null;
  
UPDATE dev_edw.customers a,dev_stg.customers b
SET   a.src_customerNumber     =b.CUSTOMERNUMBER,
   a.customerName           =b.CUSTOMERNAME,
   a.contactLastName        =b.CONTACTLASTNAME,
   a.contactFirstName       =b.CONTACTFIRSTNAME,
   a.phone                  =b.PHONE,
   a.addressLine1           =b.ADDRESSLINE1,
   a.addressLine2           =b.ADDRESSLINE2,
   a.city                   =b.CITY,
   a.state                  =b.STATE,
   a.postalCode             =b.POSTALCODE,
   a.country                =b.COUNTRY,
   a.salesRepEmployeeNumber =b.SALESREPEMPLOYEENUMBER,
   a.creditLimit            =b.CREDITLIMIT,
   a.src_update_timestamp   =b.update_timestamp,
   a.dw_update_timestamp    =CURRENT_TIMESTAMP,
   a.etl_batch_no           ='${ETL_BATCH_NO}',
   a.etl_batch_date         ='${ETL_BATCH_DATE}'
where a.src_customerNumber=b.CUSTOMERNUMBER;
''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table")

    except Exception as e:
        print(f"Error uploading to Redshift: {e}")

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
