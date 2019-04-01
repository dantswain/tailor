'''
ETL Module

This module contains code that can read a slack export and store the data as
parquet for convenient usage with spark

To use this module from the command line:
    python tailor/etl.py path/to/raw_data path/to/parquet_data
'''

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
    '''
    Runs the ETL operation

    data_dir is a location of the unzipped slack export (json files),
    parquet_dir is the location where parquet files will be written
    '''

    # build a spark context to use
    spark = _local_spark()

    # load the dataframes
    events_df = load_events(spark, data_dir)
    users_df = load_users(spark, data_dir)
    channels_df = load_channels(spark, data_dir)

    # write each dataframe
    _write_parquet(users_df, os.path.join(parquet_dir, 'users.parquet'))
    _write_parquet(channels_df, os.path.join(parquet_dir, 'channels.parquet'))

    # NOTE if this was a larger-scale system we would probably want to partition
    # the event data by date
    _write_parquet(events_df, os.path.join(parquet_dir, 'events.parquet'))


def load_events(spark: SparkSession, data_dir: str) -> DataFrame:
    '''
    Load chat events to a dataframe

    Note this works by loading all of the events into memory so that a schema
    can be inferred.  This approach would not scale to a huge export; a scalable
    approach would require us to choose and enforce a schema (which is something
    we should do for a longer-living project).
    '''
    files = glob.glob(data_dir + '/*/*.json')
    all_json = []
    for file in files:
        # we add channel and date to each row
        channel, date = _channel_and_date_from_path(file)
        with open(file, 'r') as f:
            data = json.load(f)
            strs = [json.dumps({**d, 'date': date, 'channel': channel})
                    for d in data]
            all_json.extend(strs)

    return spark.read.json(spark.sparkContext.parallelize(all_json))


def load_users(spark: SparkSession, data_dir: str) -> DataFrame:
    '''
    Load the users.json file into a dataframe (schema inferred)
    '''
    return _single_json_file_to_df(spark, os.path.join(data_dir, 'users.json'))


def load_channels(spark: SparkSession, data_dir: str) -> DataFrame:
    '''
    Load the channels.json file into a dataframe (schema inferred)
    '''
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


def _write_parquet(df, path) -> None:
    print('Writing {} rows to {}'.format(df.count(), path))
    _rm_rf_dir(path)
    df.write.parquet(path)


if __name__ == "__main__":
    run_etl(sys.argv[1], sys.argv[2])
