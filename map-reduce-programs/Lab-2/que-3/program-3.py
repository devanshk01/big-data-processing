from mrjob.job import MRJob
import csv, re

class yearlySales(MRJob):
    def mapper(self, _, line):
        if not line.strip(): return

        try:
            r = next(csv.reader([line]))
        except:
            return

        if r and r[0].strip().lower() == 'ordernumber': return

        order_date = r[4].strip()   # OrderDate (index 4)
        q = float(r[12].strip())    # Order Quantity (index 12)
        p = float(r[14].strip())    # Unit Price (index 14)
        m = re.search(r'(\d{4})', order_date)   # extract first 4-digit year

        if not m: return
        year = int(m.group(1))
        
        yield year, (1, q * p)

    def reducer(self, k, vals):
        count = 0
        total = 0.0
        for c, amt in vals:
            count += int(c)
            total += float(amt)
            
        yield k, (count, round(total, 2))

if __name__ == '__main__':
    yearlySales.run()
  
# ----- Command to run the script ----- #  
    
# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar `
# -files program-3.py `
# -input /input/orders.csv `
# -output /output/lab-2/que-3 `
# -mapper "python program-3.py --step-num=0 --mapper" `
# -reducer "python program-3.py --step-num=0 --reducer"