from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Lab-4').setMaster('local[*]')
sc = SparkContext(conf = conf)

# FOR EASIER VISUAL REPRESENTATION

def printList(l):
    for item in l:
        if not isinstance(item , (list, tuple, set, dict)):
            print(item)
        else:
            for i in item:
                print(f'{i:<20}', end = '')
            print()
            
# --------------------------------------------------------------------------------#
            
# 1. Project and print (empno, name) of employees that are from state TX from employee.csv

file = 'employee.csv'

lines = sc.textFile(file)
emps = lines.map(lambda line : line.split(','))   # RDD containing all the employees
filtered_emps = emps.filter(lambda emp : emp[4] == 'TX')  # employees from TX
final = filtered_emps.map(lambda emp : (emp[0], emp[1]))  # getting empNo and names from filtered_emps

printList(final.collect())      # --> output-1.txt

# --------------------------------------------------------------------------------#

# 2. Generate a List of (empno, name, salary, dep_avg_sal) of employees who have salary > 1.5 times of department average from employee.csv

emps_mod = emps.map(lambda emp : (emp[2], (float(emp[3]), 1.0)))
sal_dep_wise = emps_mod.reduceByKey(
    lambda a, b : (a[0] + b[0], a[1] + b[1])
    )   # sum of salaries department wise
avg_sal_dep_wise = sal_dep_wise.map(
    lambda dep : (dep[0], round(dep[1][0] / dep[1][1], 2))
    )   # avg sal per department

# avg_sal_dep_wise.collect()

emps_mod = emps.map(lambda emp : (emp[2], emp))
joinedEmp = emps_mod.join(avg_sal_dep_wise)   # RDD with emps info and avg_sal of their dep.
filtered_emps = joinedEmp.filter(
    lambda emp : float(emp[1][0][3]) > 1.5 * emp[1][1]
    )
final = filtered_emps.map(
  lambda emp : (emp[1][0][0], emp[1][0][1], emp[1][0][3], emp[1][1])
  )

printList(final.collect())      # --> output-2.txt

# --------------------------------------------------------------------------------#

# 3. Compute state-wise count of employees from employee.csv

emps_mod = emps.map(lambda emp : (emp[4], 1))
emps_count = emps_mod.reduceByKey(lambda a, b : a + b)

printList(emps_count.collect())     # --> output-3.txt

# --------------------------------------------------------------------------------#

# 4. Compute the Standard Deviation of salary

import math

sum_sals = emps.map(lambda emp : float(emp[3]))
total_sals = sum_sals.reduce(lambda sal1, sal2 : sal1 + sal2)
total_emps = emps.count()
avg_sal = total_sals / total_emps

sq_diff = sum_sals.map(lambda sal : (sal - avg_sal) ** 2)
sum_sq_sal = (sq_diff.reduce(lambda sal1, sal2 : sal1 + sal2))
std_dev_sal = math.pow(sum_sq_sal / (total_emps - 1), 0.5)  # for better accuracy

print('Standard Deviation of Salaries :', round(std_dev_sal, 2))        # --> output-4.txt

# --------------------------------------------------------------------------------#

# 5. Compute Department wise Standard Deviation of Salary

joinedEmp_mod = joinedEmp.map(
    lambda emp : (emp[0], ((float(emp[1][0][3]) - emp[1][1]) ** 2, 1))
    )
sum_sq_sal_dep_wise = joinedEmp_mod.reduceByKey(
    lambda a, b : (a[0] + b[0], a[1] + b[1])
    )
std_dev_sal_dep_wise = sum_sq_sal_dep_wise.map(
    lambda dep : (dep[0], round(
        math.pow(dep[1][0] / (dep[1][1] - 1), 0.5),
        2
        ) if dep[1][1] > 1 else 0.0)
)

printList(std_dev_sal_dep_wise.collect())       # --> output-5.txt

# --------------------------------------------------------------------------------#

# 6. List (empno, dno, name, salary, dept_sal_sd) for employee that are having salary > 1.5 times of SD.

emps_mod = emps.map(lambda emp : (emp[2], emp))
joinedEmp = emps_mod.join(std_dev_sal_dep_wise)

filtered_emps = joinedEmp.filter(
    lambda emp : float(emp[1][0][3]) > 1.5 * emp[1][1]
    )
final = filtered_emps.map(
    lambda emp:(emp[1][0][0], emp[0], emp[1][0][1], emp[1][0][3], emp[1][1])
    )

printList(final.collect())      # --> output-6.txt

# --------------------------------------------------------------------------------#

# 7. Compute how much offset each department's average salary from the overall average.

dep_offset = avg_sal_dep_wise.map(
    lambda dep : (dep[0], round(dep[1], 2), round(dep[1] - avg_sal, 2))
    )

printList(dep_offset.collect())     # --> output-7.txt

# --------------------------------------------------------------------------------#