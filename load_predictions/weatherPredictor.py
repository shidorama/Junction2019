import requests
import xmltodict
from datetime import datetime
import pandas as pd


class WeatherPredictor(object):
    TEMP_FORECAST = 'data/weather_prediction.csv'
    HIST_DATA = 'data/history_daily.csv'
    features = ['cloud', 'pressure', 'humidity', 'percip', 'show_depth', 'temp', 'dewpoint', 'visib', 'wind_dir', 'gust_speed', 'wind_speed', 'visits', 'cordNorth', 'coordSouth']

    def __init__(self):
        self._forecast_window = 10
        self.temp_10d = pd.read_csv(self.TEMP_FORECAST)
        self.hist_data = pd.read_csv(self.HIST_DATA)

    def get_last_hist_date(self, df):
        rows = []
        for counterId in df['counterId'].unique():
            df_slice = df[df['counterId'] == counterId]
            df_slice2 = df_slice[df_slice['date_day'] == max(df_slice['date_day'])]
            rows.append(df_slice2)
        return pd.concat(rows)

    def predict(self):
        hist = self.hist_data
        # df = self.temp_10d.merge(hist)
        last_hist = self.get_last_hist_date(hist)
