import json
import glob
import os

from typing import List, Any, Tuple

from collections import OrderedDict
from pyspark.sql import Row, DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.session import SparkSession


def _channel_and_date_from_path(path: str) -> Tuple[str, str]:
    _, _, channel, fn = path.split('/')
    date = os.path.splitext(fn)[0]
    return channel, date


def raw_to_df(spark: SparkSession, data_dir: str) -> DataFrame:
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
    with open(os.path.join(data_dir, 'users.json'), 'r') as f:
        user_data = json.load(f)
        return spark.read.json(spark.sparkContext.parallelize([json.dumps(u) for u in user_data]))


def load_channels(spark: SparkSession, data_dir: str) -> DataFrame:
    with open(os.path.join(data_dir, 'channels.json'), 'r') as f:
        user_data = json.load(f)
        return spark.read.json(spark.sparkContext.parallelize([json.dumps(u) for u in user_data]))
