import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import count
import sys
import os
from datetime import datetime, timedelta
import subprocess

spark = SparkSession.builder \
    .appName("LogAggregation") \
    .getOrCreate()


def aggregate_logs(start_date, input_dir):
    all_data = None
    for i in range(7):
        day = start_date + timedelta(days=i)
        file_name = os.path.join(input_dir, f"{day.strftime('%Y-%m-%d')}.csv")
        if os.path.exists(file_name):
            data = spark.read.csv(file_name, header=False, inferSchema=True)
            if all_data is None:
                all_data = data
            else:
                all_data = all_data.union(data)

    if all_data is None:
        return None

    new_columns = ["email", "action", "dt"]
    df = all_data.toDF(*new_columns)
    agg_df = df.groupBy("email") \
        .pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]) \
        .agg(count("*")) \
        .fillna(0) \
        .withColumnRenamed("CREATE", "create_count") \
        .withColumnRenamed("READ", "read_count") \
        .withColumnRenamed("UPDATE", "update_count") \
        .withColumnRenamed("DELETE", "delete_count")
    return agg_df


def write_single_csv(df, output_file):
    temp_dir = "temp_output"
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    df.coalesce(1).write.csv(temp_dir, header=True, mode='overwrite')
    temp_csv_file = None
    for filename in os.listdir(temp_dir):
        if filename.endswith(".csv"):
            temp_csv_file = filename
            break

    temp_csv_path = os.path.join(temp_dir, temp_csv_file)
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    shutil.move(temp_csv_path, output_file)
    shutil.rmtree(temp_dir)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <YYYY-mm-dd>")
        sys.exit(1)

    target_date = sys.argv[1]
    try:
        target_date = datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Use YYYY-mm-dd.")
        sys.exit(1)

    start_date = target_date - timedelta(days=7)
    input_dir = '/opt/airflow/input'
    output_dir = '/opt/airflow/output'
    result = aggregate_logs(start_date, input_dir)
    if result is not None:
        output_file = os.path.join(output_dir, f"{target_date.strftime('%Y-%m-%d')}.csv")
        write_single_csv(result, output_file)
    else:
        print("No data available for the specified period.")
