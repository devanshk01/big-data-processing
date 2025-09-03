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

#  1. Computes following monthly summary from the web access log file:
# (a) Total number of requests.
# (b) Total download size (in MegaBytes).

file = 'web_access_log.txt'
lines = sc.textFile(file)

logs = lines.map(lambda line : line.split(' '))

# a) total requests
logs_month_wise = logs.map(lambda log : (log[3].split('/')[1], 1))
total_requests = logs_month_wise.reduceByKey(lambda a, b : a + b)

# b) total download size
logs_size_wise = logs.map(
    lambda log : (log[3].split('/')[1], float(log[8]))
    )
total_download_size = logs_size_wise.reduceByKey(lambda a, b : a + b)

final = total_requests.join(total_download_size)
final = final.map(
    lambda log : (log[0], log[1][0], round(log[1][1] / (1024 * 1024), 2))
    )

printList(final.collect())      # --> output-1.txt

# --------------------------------------------------------------------------------#

# 2. Find the most popular browser (made most requests) from the “web access log” file.

browser_count = logs.map(lambda log : (log[-3].split('/')[0], 1))
browser_count = browser_count.reduceByKey(lambda log1, log2 : log1 + log2)

most_popular_browser = browser_count.max(lambda log : log[1])

print(most_popular_browser)     # --> output-2.txt

# --------------------------------------------------------------------------------#