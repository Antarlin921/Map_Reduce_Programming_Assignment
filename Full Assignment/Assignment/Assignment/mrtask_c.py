# What are the different payment types used by customers and their count? The final results should be in a sorted format.

""" A numeric code signifying how the passenger paid for the trip.

1= Credit card

2= Cash

3= No charge

4= Dispute

5= Unknown

6= Voided trip
 """

from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMapReduce(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.sort_by_count)
        ]

    def mapper(self, _, line):
        if(not line.startswith('VendorID')):
           payment_type=line.split(',')[9]
           if(payment_type=='1'):
               payment_type='Credit card'
           if(payment_type=='2'):
               payment_type='Cash'
           if(payment_type=='3'):
               payment_type='No charge'
           if(payment_type=='4'):
               payment_type='Dispute'
           if(payment_type=='5'):
               payment_type='Unknown'
           if(payment_type=='6'):
               payment_type='Voided trip'
           
           yield (payment_type,1)

    def reducer(self, payment_type, value):
        yield None, (sum(value),payment_type)

    def sort_by_count(self, _,pair):
        sorted_pairs = sorted(pair, reverse = True)
        for pair in sorted_pairs:
            yield (pair[1],pair[0])

# 
if __name__ == '__main__':
    MyMapReduce.run()
