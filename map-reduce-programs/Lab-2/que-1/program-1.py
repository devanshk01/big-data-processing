from mrjob.job import MRJob
import csv

class salesChannelCount(MRJob):
    def mapper(self, _, line):
        if not line.strip():
            return
        
        try:
            row = next(csv.reader([line]))
        except Exception:
            return
        
        if len(row) > 0 and row[0].strip().lower() == "ordernumber":
            return
        
        if len(row) < 0:
            return
        
        order_number = row[0].strip()
        sales_channel = row[1].strip() or 'UNKNOWN'    # fallback to UNKNOWN if empty
        
        yield sales_channel, order_number
        
    def reducer(self, sales_channel, order_numbers):
        count = sum(1 for _ in order_numbers)
        yield sales_channel, count

        
if __name__ == "__main__":
    salesChannelCount.run()
    
# ----- Command to run the script ----- #

# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar `
# -files program-1.py `
# -input /input/orders.csv `
# -output /output/lab-2/que-1 `
# -mapper "python program-1.py --step-num=0 --mapper" `
# -reducer "python program-1.py --step-num=0 --reducer"