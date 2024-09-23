import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum
import sys
import os
from datetime import datetime, timedelta

spark = SparkSession.builder \
    .appName("LogAggregation") \
    .getOrCreate()


def aggregate_daily_data(date, input_dir, temp_dir):
    file_name = os.path.join(input_dir, f"{date.strftime('%Y-%m-%d')}.csv")
    data = spark.read.csv(file_name, header=False, inferSchema=True)
    new_columns = ["email", "action", "dt"]
    df = data.toDF(*new_columns)
    agg_df = df.groupBy("email") \
        .pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]) \
        .agg(count("*")) \
        .fillna(0) \
        .withColumnRenamed("CREATE", "create_count") \
        .withColumnRenamed("READ", "read_count") \
        .withColumnRenamed("UPDATE", "update_count") \
        .withColumnRenamed("DELETE", "delete_count")

    output_file = os.path.join(temp_dir, f"{date.strftime('%Y-%m-%d')}.csv")
    write_single_csv(agg_df, output_file)


def aggregate_weekly_data(start_date, input_dir, temp_dir):
    weekly_data = None

    for i in range(7):
        day = start_date + timedelta(days=i)
        file_name = os.path.join(temp_dir, f"{day.strftime('%Y-%m-%d')}.csv")

        if not os.path.exists(file_name):
            aggregate_daily_data(day, input_dir, temp_dir)

        data = spark.read.csv(file_name, header=True, inferSchema=True)

        if weekly_data is None:
            weekly_data = data
        else:
            weekly_data = weekly_data.union(data)

    if weekly_data is None:
        return None

    weekly_agg = weekly_data.groupBy("email") \
        .agg(
        sum("create_count"),
        sum("read_count"),
        sum("update_count"),
        sum("delete_count")
        ) \
        .withColumnRenamed("sum(create_count)", "create_count") \
        .withColumnRenamed("sum(read_count)", "read_count") \
        .withColumnRenamed("sum(update_count)", "update_count") \
        .withColumnRenamed("sum(delete_count)", "delete_count")

    return weekly_agg


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
    temp_dir = '/opt/airflow/temp'
    result = aggregate_weekly_data(start_date, input_dir, temp_dir)
    if result is not None:
        output_file = os.path.join(output_dir, f"{target_date.strftime('%Y-%m-%d')}.csv")
        write_single_csv(result, output_file)
    else:
        print("No data available for the specified period.")
