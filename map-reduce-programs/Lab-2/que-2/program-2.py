from mrjob.job import MRJob
import csv

class salesChannelCount2(MRJob):
    def mapper(self, _, line):
        if not line.strip(): return
        
        try:
            r = next(csv.reader([line]))
        except: return
        
        if r and r[0].strip().lower() == 'ordernumber': return
        
        ch = r[1].strip() or 'UNKNOWN'
        q, p = float(r[12].strip()), float(r[14].strip())
        
        yield ch, (1, q * p)

    def reducer(self, k, vals):
        count = 0
        total = 0.0
        
        for c, amt in vals:
            count += int(c)
            total += float(amt)
        
        yield k, (count, round(total, 2))

if __name__ == '__main__':
    salesChannelCount2.run()


# ----- Command to run the script ----- #

# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar `
# -files program-2.py `
# -input /input/orders.csv `
# -output /output/lab-2/que-2 `
# -mapper "python program-2.py --step-num=0 --mapper" `
# -reducer "python program-2.py --step-num=0 --reducer"