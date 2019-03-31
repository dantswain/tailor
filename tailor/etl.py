import json
import glob
import os
import shutil
import sys

from typing import List, Any, Tuple

from pyspark import SparkConf
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.session import SparkSession


def run_etl(data_dir: str, parquet_dir: str):
    spark = _local_spark()
    events_df = load_events(spark, data_dir)
    users_df = load_users(spark, data_dir)
    channels_df = load_channels(spark, data_dir)

    print('Writing {} users'.format(users_df.count()))
    users_path = os.path.join(parquet_dir, 'users.parquet')
    _rm_rf_dir(users_path)
    users_df.write.parquet(users_path)

    print('Writing {} channels'.format(channels_df.count()))
    channels_path = os.path.join(parquet_dir, 'channels.parquet')
    _rm_rf_dir(channels_path)
    channels_df.write.parquet(channels_path)

    print('Writing {} events'.format(events_df.count()))
    events_path = os.path.join(parquet_dir, 'events.parquet')
    _rm_rf_dir(events_path)
    events_df.write.partitionBy('date').parquet(events_path)


def load_events(spark: SparkSession, data_dir: str) -> DataFrame:
    files = glob.glob(data_dir + '/*/*.json')
    all_json = []
    for file in files:
        channel, date = _channel_and_date_from_path(file)
        with open(file, 'r') as f:
            data = json.load(f)
            strs = [json.dumps({**d, 'date': date, 'channel': channel})
                    for d in data]
            all_json.extend(strs)

    return spark.read.json(spark.sparkContext.parallelize(all_json))


def load_users(spark: SparkSession, data_dir: str) -> DataFrame:
    return _single_json_file_to_df(spark, os.path.join(data_dir, 'users.json'))


def load_channels(spark: SparkSession, data_dir: str) -> DataFrame:
    return _single_json_file_to_df(spark, os.path.join(data_dir, 'channels.json'))


def _rm_rf_dir(path: str) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def _local_spark() -> SparkSession:
    conf = SparkConf()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def _channel_and_date_from_path(path: str) -> Tuple[str, str]:
    _, _, channel, fn = path.split('/')
    date = os.path.splitext(fn)[0]
    return channel, date


def _single_json_file_to_df(spark: SparkSession, json_path: str) -> DataFrame:
    with open(json_path, 'r') as f:
        json_data = json.load(f)
        return spark.read.json(spark.sparkContext.parallelize([json.dumps(j) for j in json_data]))


if __name__ == "__main__":
    run_etl(sys.argv[1], sys.argv[2])
