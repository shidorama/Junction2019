import requests
import xmltodict
from datetime import datetime
import pandas as pd


class WeatherForecastDownloader(object):
    URL = 'http://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::multipointcoverage&{location}&timestep=1440&starttime={}&endtime={}'

    def __init__(self, location='place=helsinki', starttime, endtime):
        self.url = URL.format(location=location)

    def load(self):
        pass

url = 'http://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::multipointcoverage&place=helsinki&starttime=2019-11-16T10:00:00Z&endtime=2019-11-17T21:00:00Z'


class WeatherKey(object):
    def _convert_ts(self, val):
        return datetime.utcfromtimestamp(int(val)).strftime('%Y-%m-%d %H:%M:%S')

    def __init__(self, d):
        self.x = d['x']
        self.y = d['y']
        self.time = self._convert_ts(d['timestamp'])

    def __repr__(self):
        return 'x: {}; y: {}; time: {}'.format(self.x, self.y, self.time)

    def to_dict(self):
        return {'x': self.x, 'y': self.y, 'time': self.time}


class WeatherFeatures(object):
    def __init__(self, key, predictions):
        self.key = key
        self.predictions = predictions

    def to_dict(self):
        return dict(self.key.to_dict(), **self.predictions)


def read_file(name):
    with open(name, 'r') as f:
        return f.read()


def get_om_result(raw_forecast):
    return raw_forecast['wfs:FeatureCollection']['wfs:member']['omso:GridSeriesObservation']['om:result'] \
        ['gmlcov:MultiPointCoverage']


def parse_features(raw_forecast):
    fields = get_om_result(raw_forecast)['gmlcov:rangeType']['swe:DataRecord']['swe:field']
    return  [raw['@name'] for raw in fields]


def parse_raw_text_values(line):
    return [[float(v.strip()) for v in row.split()] for row in line.split('\n')]


def map_keys_values(key_features, values):
    return [{k: v for k, v in zip(key_features, row)} for row in values]


def parse_weather_keys(key_features, raw_forecast):
    fields = get_om_result(raw_forecast)['gml:domainSet']['gmlcov:SimpleMultiPoint']['gmlcov:positions']
    values = parse_raw_text_values(fields)
    mapping = map_keys_values(key_features, values)
    return [WeatherKey(raw_point) for raw_point in mapping]


def parse_predictions(key_features, raw_forecast):
    fields = get_om_result(raw_forecast)['gml:rangeSet']['gml:DataBlock']['gml:doubleOrNilReasonTupleList']
    values = parse_raw_text_values(fields)
    mapping = map_keys_values(key_features, values)
    return mapping


def parse_forecast(xml):
    d = xmltodict.parse(xml)
    weather_keys = parse_weather_keys(['x', 'y', 'timestamp'], d)
    features = parse_features(d)
    predictions = parse_predictions(features, d)
    return [WeatherFeatures(weather_key, prediction) for weather_key, prediction in zip(weather_keys, predictions)]


def get_forecast():
    xml = read_file('forecast.xml')
    return parse_forecast(xml)


def forecast_to_csv(forecast, filepath):
    forecast = get_forecast()
    df = pd.DataFrame([row.to_dict() for row in forecast])
    df.to_csv(filepath, index=False)


if __name__ == '__main__':
    forecast = get_forecast()
    forecast_to_csv(forecast, 'weather_forecast_helsinki.csv')
