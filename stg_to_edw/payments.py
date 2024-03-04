import psycopg2

# Redshift credentials
redshift_host = "default-workgroup.834787109995.us-east-1.redshift-serverless.amazonaws.com"
redshift_port = "5439"
redshift_database = "dev"
redshift_user = "admin"
redshift_password = "BizAct#12345"


def connect_to_redshift(host, port, database, user, password):
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

        copy_command= ('''update dev_edw.payments a,dev_stg.payments b
set
a.checkNumber         =b.checknumber,
   a.paymentDate         =b.paymentdate,
   a.amount              =b.amount,
   a.src_update_timestamp=b.update_timestamp,
   a.dw_update_timestamp=current_timestamp,
   a.etl_batch_no          ='${ETL_BATCH_NO}',
   a.etl_batch_date        ='${ETL_BATCH_DATE}'
where a.checkNumber=b.checkNumber;


insert into dev_edw.payments
(
 dw_customer_id        ,
   src_customerNumber  ,
   checkNumber         ,
   paymentDate         ,
   amount              ,
   src_create_timestamp ,
   src_update_timestamp,
   etl_batch_no,
   etl_batch_date 
)
SELECT
  c.dw_customer_id
, p.CUSTOMERNUMBER
, p.CHECKNUMBER
, p.PAYMENTDATE
, p.AMOUNT
, p.create_timestamp
, p.update_timestamp
,'${ETL_BATCH_NO}'
,'${ETL_BATCH_DATE}'
FROM dev_stg.payments p left join dev_edw.payments p1 on p.checkNumber=p1.checkNumber
inner join  dev_edw.customers c on c.src_customernumber=p.customernumber
where p1.checkNumber is null;
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
    redshift_password
)
