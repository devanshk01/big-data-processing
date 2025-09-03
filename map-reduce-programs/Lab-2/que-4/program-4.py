from mrjob.job import MRJob

class MonthlyStats(MRJob):
    def mapper(self, _, line):
        p = line.split()
        if len(p) < 10: return

        ym = p[3][4:7] + '-' + p[3][8:12]  # Dec-2015

        try: 
          size = int(p[9])
        except: 
          size = 0

        yield ym, (1, size)

    def reducer(self, k, vals):
        c = s = 0
        
        for a, b in vals:
            c += a; s += b
        
        yield k, (c, round(s/(1024*1024), 2))

if __name__ == '__main__':
    MonthlyStats.run()
    
# ----- Command to run the script ----- #  
    
# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar `
# -files program-4.py `
# -input /input/web_access_log.txt `
# -output /output/lab-2/que-4 `
# -mapper "python program-4.py --step-num=0 --mapper" `
# -reducer "python program-4.py --step-num=0 --reducer"