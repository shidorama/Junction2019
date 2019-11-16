import csv
import datetime

import pyspark
from pyspark.sql import SparkSession

from schemas import weather_enriched_schema, weather_enriched_schema_holidays

tz = datetime.timezone(datetime.timedelta(hours=2))

holidays = []

with open("holidays.csv") as fp:
    for line in fp:
        raw_fields = line.split(",")
        raw_date = datetime.datetime.strptime(raw_fields[0], "%d/%m/%Y")
        raw_date = raw_date.replace(tzinfo=tz)
        if raw_fields[3] == 'National holiday\n':
            holidays.append(raw_date)


def mark_holiday(line: str):
    ds_fields: list = line.split(',')
    print(len(ds_fields))
    print("search for holidays")
    start_date = datetime.datetime.fromtimestamp(float(ds_fields[0]), datetime.timezone(datetime.timedelta(hours=2)))
    holiday = False
    for date in holidays:
        delta = start_date - date
        if delta.total_seconds() > 0 and delta < datetime.timedelta(days=1):
            holiday = True

    ds_fields.append(holiday)
    return ds_fields


sc = pyspark.SparkContext('local[*]')
ss = SparkSession(sc)

base_rdd = sc.textFile('file://///home/ANT.AMAZON.COM/stanza/PycharmProjects/Junction2019/holidays/result.csv')
results = base_rdd.filter(lambda line: line.split(',')[0] != 'startDate').map(mark_holiday).collect()

#         fp.write(dumps(line) + "\n")
with open("result_holiday.csv", 'w') as csvfp:
    writer = csv.writer(csvfp)
    writer.writerow(weather_enriched_schema_holidays)
    writer.writerows(results)
# ss.createDataFrame(base_rdd, weather_enriched_schema)
# ss.
