from mrjob.job import MRJob
import math as mt

class mrjob_iris(MRJob):
    def configure_args(self): # this is the first comment I can write
        super(mrjob_iris, self).configure_args() # now this is the second
        self.add_file_arg('--class-file', help = 'class vector file') # go ahead and write the 3rd one
        
    def euclidean_distance(self, vector_1, vector_2):
        return mt.sqrt(sum((a - b) ** 2 for a, b in zip(vector_1, vector_2)))
    
    def mapper_init(self):
        self.classes = []
        
        with open(self.options.class_file, 'r') as file:
            next(file)  # skipping header
            
            for line in file:
                values = [float(x) for x in line.strip().split(',')]
                self.classes.append(values)
    
    def mapper(self, _, line):
        fields = line.strip().split(',')
        
        if fields[0] == 'sepal_length':
            return 
        
        sample = [float(x) for x in fields]
        nearest_class = None
        shortest_distance = float('inf')
        
        for index, class_vector in enumerate(self.classes):
            distance = self.euclidean_distance(sample, class_vector)
            
            if distance < shortest_distance:
                shortest_distance = distance
                nearest_class = index + 1
                
        yield None, nearest_class
        
    def reducer(self, _, class_ids):
        for class_id in class_ids:
            yield 'class_id = ', class_id
                
if __name__ == '__main__':
    mrjob_iris.run()
    
    
# ----- Execution command ----- #
#  python mrjob-iris.py iris.csv --class-file iris_classes.csv > output-1.txt