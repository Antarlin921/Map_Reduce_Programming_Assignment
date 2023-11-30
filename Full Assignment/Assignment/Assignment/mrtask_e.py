# Calculate the average tips to revenue ratio of the drivers for different pickup locations in sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MyMapReduce(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,combiner=self.combiner,reducer=self.reducer),
            MRStep(reducer=self.sort_by_tip_revenue_ratio)
        ]

    def mapper(self, _, line):
        if(not line.startswith('VendorID')):
           PULocationID=line.split(',')[7]
           tip_amt=float(line.split(',')[13])
           total_amt=float(line.split(',')[16])
           yield PULocationID, (tip_amt,total_amt)

    def combiner(self,PULocationID, tip_amt):
        total_tip_amt = 0
        total_amt = 0
        for tip_am, total_am in tip_amt:
            total_tip_amt += tip_am
            total_amt += total_am
        yield PULocationID, (total_tip_amt,total_amt)
    
    
    def reducer(self,PULocationID, tip_amt):
        total_tip_amt = 0
        total_amt = 0
        for tip_am, total_am in tip_amt:
            total_tip_amt += tip_am
            total_amt += total_am
        total_tip_revenue_ratio=total_tip_amt/total_amt
        yield None, (total_tip_revenue_ratio,PULocationID)
    
    def sort_by_tip_revenue_ratio(self, _,pair):
        sorted_pairs = sorted(pair, reverse = True)
        for pair in sorted_pairs:
            yield (pair[1],pair[0])
# 
if __name__ == '__main__':
    MyMapReduce.run()
