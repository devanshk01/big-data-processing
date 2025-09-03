from mrjob.job import MRJob

class NotFoundURLs(MRJob):
    def mapper(self, _, line):
        p = line.split()
        if len(p) < 9: return
        
        status = p[8]
        
        if status == '404':
            timestamp = p[3][1:]          # remove leading '['
            url = p[6]                    # requested resource
            
            yield timestamp, url

    def reducer(self, ts, urls):
        for u in urls:
            yield ts, u

if __name__ == '__main__':
    NotFoundURLs.run()

# ----- Command to run the script ----- #  
    
# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar `
# -files program-5.py `
# -input /input/web_access_log.txt `
# -output /output/lab-2/que-5 `
# -mapper "python program-5.py --step-num=0 --mapper" `
# -reducer "python program-5.py --step-num=0 --reducer"