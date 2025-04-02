from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, count, desc

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("NASA-Log-Analyzer") \
    .getOrCreate()

# Step 2: Load the NASA log file into a DataFrame
# log_file = "data/server_logs.txt"  
log_file = "data/NASA_access_log_Aug95"
# log_file = "data/NASA_access_log_Jul95"
logs_df = spark.read.text(log_file)

# Step 3: Parse the logs (assuming NASA Common Log Format)
parsed_logs = logs_df.select(
    split(col("value"), " ")[0].alias("ip"),  
    split(col("value"), " ")[3].alias("timestamp"),  
    split(col("value"), " ")[6].alias("request"),  
    split(col("value"), " ")[8].alias("status"),  
    split(col("value"), " ")[9].cast("int").alias("bytes")  
)

# Step 4: Perform data analysis
# 4.1 Count requests by IP address
ip_request_counts = parsed_logs.groupBy("ip").agg(count("*").alias("request_count"))

# 4.2 Find top 10 IPs with the most requests
top_ips = ip_request_counts.orderBy(desc("request_count")).limit(10)

# 4.3 Count requests by HTTP status code
status_code_counts = parsed_logs.groupBy("status").agg(count("*").alias("status_count"))

# 4.4 Calculate total bytes transferred
total_bytes = parsed_logs.agg({"bytes": "sum"}).collect()[0][0]

# Step 5: Display the results
print("Top 10 IPs with the most requests:")
top_ips.show()

print("Requests by HTTP status code:")
status_code_counts.show()

print(f"Total bytes transferred: {total_bytes}")

# Step 6: Save results to files (optional)
output_dir = "output/"
top_ips.write.csv(output_dir + "top_ips.csv", header=True)
status_code_counts.write.csv(output_dir + "status_code_counts.csv", header=True)

# Step 7: Stop the SparkSession
spark.stop()