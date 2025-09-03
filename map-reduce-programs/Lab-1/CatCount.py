from mrjob.job import MRJob

class CatCount(MRJob):
    def mapper(self, key, line):
        record = line.split(',')
        category = record[2]
        yield category, 1
    
    def reducer(self, category, counts):
        total_count = sum( counts )
        if total_count > 10:
            yield category, total_count

if __name__ == '__main__':
    CatCount.run()
    
    
# Command to run the the script    

# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar `
# -files CatCount.py `
# -input /input/amazon-sales.csv `
# -output /output/catcount `
# -mapper "python CatCount.py --step-num=0 --mapper" `
# -reducer "python CatCount.py --step-num=0 --reducer"