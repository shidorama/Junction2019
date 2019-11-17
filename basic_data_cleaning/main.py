import csv
import pyspark
import pymongo
import datetime
from json import dumps, dump

"""
7/29/2018 19:00 
68/3/2017 11:00
"""
date_format = '%m/%d/%Y %H:%M'
fields = [
    "CounterReadingID", "CounterID_ASTA", "SequenceNumber", "StartTime", "EndTime", "Visits",
    "ASTA_Counters.CounterID_PAVE", "ASTA_Counters.Name_ASTA", "ASTA_Counters.InstallationDate",
    "ASTA_Counters.NationalParkCode", "ASTA_Counters.Municipality", "ASTA_Counters.RegionalUnit",
    "ASTA_Counters.RegionalEntity", "PAVE_Counters.Globalid", "PAVE_Counters.Name", "PAVE_Counters.Manager",
    "PAVE_Counters.AdditionalInfo", "PAVE_Counters.CoordinateNorth", "PAVE_Counters.CoordinateEast"
]

tz = datetime.timezone(datetime.timedelta(hours=2))
def line_processor(line):
    data = line.split('\t')

    if (len(data) < len(fields)):
        print('data length is wrong')
        print(data)
        print(line)
        raise RuntimeError()

    try:
        start_date = datetime.datetime.strptime(data[3], date_format).replace(tzinfo=tz)
        end_date = datetime.datetime.strptime(data[4], date_format).replace(tzinfo=tz)
        document = {
            "measurmentId": int(data[0]),
            "counterId": int(data[1]),
            "seqNumber": int(data[2]),
            "startDate": start_date.timestamp(),
            "endDate": end_date.timestamp(),
            "visits": int(data[5]),
            "counterIdPave": data[6],
            "installationDate": datetime.datetime.strptime(data[8], date_format).replace(tzinfo=tz).timestamp(),
            "parkCode": int(data[9]),

            "cordNorth": int(data[17]),
            "coordSouth": int(data[18])
        }
        return document
    except Exception as e:
        print('ooops')
        print(e)


sc = pyspark.SparkContext('local[*]')

csv_spark = sc.textFile('file://///home/ANT.AMAZON.COM/stanza/PycharmProjects/Junction2019/counters.csv')
python_lines = csv_spark.filter(lambda line: len(line.split('\t')) == len(fields)).map(line_processor)




result = python_lines.collect()

with open("parsed_raw.csv", "w") as fp:
    for line in result:
        if isinstance(line, dict):
            fp.write(dumps(line) + "\n")



# client = pymongo.MongoClient(uri)
# client.testdb.testcoll.insert_one({"somestuff": 1})