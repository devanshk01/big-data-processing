from mrjob.job import MRJob
from mrjob.step import MRStep

class moving_avg(MRJob):
    def configure_args(self):
        super(moving_avg, self).configure_args()
        self.add_passthru_arg(
            '--window-size',
            type = int,
            help = 'size of moving average window'
        )
        
    def steps(self):
        return [
            MRStep(
                mapper = self.map_to_month,
                reducer = self.reduce_monthly
            ),
            MRStep(
                reducer = self.calculate_moving_avg
            )
        ]
        
    def map_to_month(self, _, line):
        fields = line.strip().split(',')
        date_string = fields[3]
        
        # skipping header
        if date_string.lower() == 'date':
            return 
        
        # Extracting counts
        first_count = int(fields[6].strip('"'))
        return_count = int(fields[7].strip('"'))
        
        # Year-Month format
        month, _, year = date_string.split('/')
        year_month = f'{year}-{month}'
        
        yield year_month, (first_count, return_count)
        
    def reduce_monthly(self, year_month, counts):
        total_first, total_return = 0, 0
        
        for first, returning in counts:
            total_first += first
            total_return += returning
            
        yield None, (year_month, total_first, total_return)
        
    def calculate_moving_avg(self, _, monthly_data):
        data_list = list(monthly_data)
        
        data_list.sort(key = lambda x: (
            int(x[0].split('-')[0]),
            int(x[0].split('-')[1])
        ))  # sort by year and then month
        
        window_size = self.options.window_size
        sum_first, sum_return = 0, 0
        
        for index, (year_month, first_count, return_count) in enumerate(data_list):
            sum_first += first_count
            sum_return += return_count
            
            if index >= window_size:
                _, old_first, old_return = data_list[index - window_size]
                sum_first -= old_first
                sum_return -= old_return
                divisor = window_size
            else:
                divisor = index + 1
                
            avg_first = round(sum_first / divisor, 2)
            avg_return = round(sum_return / divisor, 2)
            
            yield year_month, (avg_first, avg_return)
            
if __name__ == '__main__':
    moving_avg.run()
    
    
# ----- Execution command ----- #
#  python mrjob-moving-window.py daily-website-visitors.csv --window-size <window size> > output-3.txt