# What is the average trip time for different pickup locations?


from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime as dt

class MyMapReduce(MRJob):

    def mapper(self, _, line):
        if(not line.startswith('VendorID')):
           PULocationID=line.split(',')[7]
           tpep_pickup_datetime=dt.strptime(line.split(',')[1],'%Y-%m-%d %H:%M:%S')
           tpep_dropoff_datetime=dt.strptime(line.split(',')[2],'%Y-%m-%d %H:%M:%S')
           trip_time=(tpep_dropoff_datetime-tpep_pickup_datetime).seconds/60
           yield PULocationID,(trip_time ,1)

    def combiner(self,PULocationID, trip_time):
        total_trip_time = 0
        total_count = 0
        for tt, count in trip_time:
            total_trip_time += tt
            total_count += count
        yield PULocationID, (total_trip_time, total_count)
# 
    def reducer(self,PULocationID, trip_time):
        total_trip_time = 0
        total_count = 0
        for tt, count in trip_time:
            total_trip_time += tt
            total_count += count
        average_trip_time = total_trip_time / total_count
        yield PULocationID, average_trip_time
# 
if __name__ == '__main__':
    MyMapReduce.run()
