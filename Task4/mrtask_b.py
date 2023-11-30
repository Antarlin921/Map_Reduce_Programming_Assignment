# Which pickup location generates the most revenue? 

from mrjob.job import MRJob
from mrjob.step import MRStep

class MyMapReduce(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.final_reducer)
        ]

    def mapper(self, _, line):
        if(not line.startswith('VendorID')):
           PULocationID=line.split(',')[7]
           total_amt=float(line.split(',')[16])
           yield (PULocationID, total_amt)

    def reducer(self,PULocationID, total_amt):
        yield None, (sum(total_amt), PULocationID)

    def final_reducer(self, _, values):
        max_total_amount, PULocationID = max(values)
        yield PULocationID, max_total_amount
# 
if __name__ == '__main__':
    MyMapReduce.run()

"""
python task_b.py input > out.txt
"""