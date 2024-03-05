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

        copy_command= (f'''INSERT INTO dev_edw.employees
(employeeNumber            ,
   lastName               ,
   firstName              ,
   extension              ,
   email                  ,
   officeCode             ,
   reportsTo              ,
   jobTitle               ,
   dw_office_id           ,
   src_create_timestamp   ,
   src_update_timestamp   ,
   etl_batch_no,
   etl_batch_date  
)
SELECT
  e.EMPLOYEENUMBER
, e.LASTNAME
, e.FIRSTNAME
, e.EXTENSION
, e.EMAIL
, e.OFFICECODE
, e.REPORTSTO
, e.JOBTITLE
, CAST(o.officecode AS INTEGER)
, e.create_timestamp
, e.update_timestamp
,'{ETL_BATCH_NO}'
,'{ETL_BATCH_DATE}'
FROM dev_stg.employees e left join dev_edw.employees e1 on e.employeeNumber=e1.employeeNumber
 inner join dev_edw.offices o on e.officecode=o.officecode
where e1.employeeNumber is null;



UPDATE dev_edw.employees a
SET dw_reporting_employee_id=b.dw_employee_id
from dev_edw.employees b
where a.reportsTo=b.employeeNumber;


UPDATE dev_edw.employees a
SET 
employeeNumber            =b.EMPLOYEENUMBER,
   lastName               =b.LASTNAME,
   firstName              =b.FIRSTNAME,
   extension              =b.EXTENSION,
   email                  =b.EMAIL,
   officeCode             =b.OFFICECODE,
   reportsTo              =b.REPORTSTO,
   jobTitle               =b.JOBTITLE,
   src_update_timestamp   =b.update_timestamp,
   dw_update_timestamp    =CURRENT_TIMESTAMP,
   etl_batch_no           ='{ETL_BATCH_NO}',
   etl_batch_date         = '{ETL_BATCH_DATE}'
   from dev_stg.employees b
where a.employeeNumber =b.employeeNumber;

''')

        cursor.execute(copy_command)

        connection.commit()

        print(f"Data uploaded to Redshift table employees")

    except Exception as e:
        print(f"Error uploading to Redshift: {e} employees")

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
