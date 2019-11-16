from time import strptime
import csv
import pyspark
from pyspark.sql import SparkSession
from datetime import datetime
from json import dumps, dump, loads

counter_schema = ['measurmentId', 'counterId', 'seqNumber', 'startDate', 'endDate', 'visits', 'counterIdPave', 'installationDate', 'parkCode', 'cordNorth', 'coordSouth']
weather_schema = ['startDate', "cloud", "pressure", "humidity", "percip", "show_depth", "temp", "dewpoint", "visib", "wind_dir", "gust_speed", "wind_speed"]

sc = pyspark.SparkContext('local[*]')
ss = SparkSession(sc)

def dict_to_df(record):
    array_record = []
    for field in counter_schema:
        array_record.append(record[field])
    return array_record

def line_to_weather(line):
    rd = line.split(',')
    time_parts = rd[3].split(":")
    time = datetime(int(rd[0]), int(rd[1]), int(rd[2]), int(time_parts[0]), int(time_parts[1]))
    result = [
        time.timestamp()
    ]
    weather_details = []
    try:
        for part in rd[5:]:
            if part == '':
                weather_details.append(None)
            else:
                weather_details.append(float(part))
    except Exception as e:
        print("Errors! %s"% e)
        print(rd[5:])
    else:
        result.extend(weather_details)
        return result


counters_rdd = sc.textFile('file://///home/ANT.AMAZON.COM/stanza/PycharmProjects/Junction2019/weather/parsed_raw.csv')
counter_lines = counters_rdd.map(lambda line: loads(line)).map(dict_to_df)
counter_df = ss.createDataFrame(counter_lines, counter_schema)

weather_rdd = sc.textFile('file://///home/ANT.AMAZON.COM/stanza/PycharmProjects/Junction2019/weather/weather_details_nuuksio.csv')
weather_lines = weather_rdd.map(line_to_weather)
weather_df = ss.createDataFrame(weather_lines, weather_schema)

result = weather_df.join(counter_df, on="startDate")

results = result.collect()

with open("result.csv", 'w') as csvfp:
    writer = csv.writer(csvfp)
    writer.writerow(result.columns)
    writer.writerows(results)

print(result.count())

