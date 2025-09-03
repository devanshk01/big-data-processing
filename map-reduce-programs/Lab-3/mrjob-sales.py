from mrjob.job import MRJob

class sales(MRJob):
    def configure_args(self):
        super(sales, self).configure_args()
        self.add_file_arg('--stores-file', help = 'stores details file')
        
    def mapper_init(self):
        self.store_info = {}  # load into dictionary
        
        with open(self.options.stores_file, 'r') as file:
            next(file) # skipping header
            
            for line in file:
                columns = line.strip().split(',')
                store_id = columns[0]
                state_code = columns[4]
                self.store_info[store_id] = state_code
            
    def mapper(self, _, transaction_line):
        fields = transaction_line.strip().split(',')
        store_id = fields[10]
        
        # skipping header
        if store_id.lower() == 'storeid':
            return 
        
        quantity = int(fields[12])
        unit_price = float(fields[14])
        
        if store_id in self.store_info:
            state_code = self.store_info[store_id]
            total_sales = (quantity * unit_price)
            
            yield state_code, total_sales
            
    def reducer(self, state_code, sales):
        total_revenue = sum(sales)
        
        yield state_code, total_revenue
        
if __name__ == '__main__':
    sales.run()
    
    
# ----- Execution command ----- #
#  python mrjob-sales.py orders.csv --stores-file stores.csv > output-2.txt