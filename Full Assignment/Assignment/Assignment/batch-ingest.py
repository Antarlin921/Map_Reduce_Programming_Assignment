import happybase

# create connection
con = happybase.Connection('localhost')

# open connection
def open_connection():
    con.open()
    print("Connection opened")

# close connection 
def close_connection():
    con.close()
    print("Connection closed")

# find the table
def get_table(name):
    open_connection()
    table = con.table(name)
    close_connection()
    return table

# Insert Data from File to Hbase Table
def batch_insert_data(filename, tablename):
    print("batch insert start : "+filename)
    file = open(filename, 'r')
    print(file)
    table = get_table(tablename)
    open_connection()
    i = 0
    with table.batch(batch_size=10000) as b:
        for line in file:
            print("Batch Processing Start for Line : "+str(i))
            if i!=0:
                temp = line.strip().split(",")
                b.put(temp[0]+temp[1]+temp[2] ,{'trip_data:VendorID': str(temp[0]), 'trip_data:tpep_pickup_datetime': str(temp[1]), 'trip_data:tpep_dropoff_datetime': str(temp[2]), 'trip_data:passenger_count': str(temp[3]), 'trip_data:trip_distance': str(temp[4]), 'trip_data:RatecodeID': str(temp[5]), 'trip_data:store_and_fwd_flag': str(temp[6]), 'trip_data:PULocationID': str(temp[7]), 'trip_data:DOLocationID': str(temp[8]), 'trip_data:payment_type': str(temp[9]),'trip_data:fare_amount': str(temp[10]), 'trip_data:extra': str(temp[11]), 'trip_data:mta_tax': str(temp[12]), 'trip_data:tip_amount': str(temp[13]), 'trip_data:tolls_amount': str(temp[14]), 'trip_data:improvement_surcharge': str(temp[15]), 'trip_data:total_amount': str(temp[16]),'trip_data:congestion_surcharge': str(temp[17]), 'trip_data:airport_fee': str(temp[18]) })
                print("Batch Processing end for Line : "+str(i))
            i+=1

    file.close()
    print("Batch insert completed sucessfully : " +filename)
    close_connection()

# Insert data from file 3
batch_insert_data('/home/hadoop/yellow_tripdata_2017-03.csv', 'nyc_taxi_hbase')

# Insert data from file 4
batch_insert_data('/home/hadoop/yellow_tripdata_2017-04.csv', 'nyc_taxi_hbase')
