import numpy as np
import pandas as pd
import lightgbm as lgb
from fbprophet import Prophet
from datetime import datetime
from datetime import date
from datetime import timedelta
import os
os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"


class BusynessPredictor(object):
    HIST_DATA = 'data/history_daily.csv'
    HIST_HOUR = 'data/history_hour.csv'
    OUTP_DATA = 'data/daily_prediction.csv'
    OUTP_DATA2 = 'data/day_part_prediction.csv'

    def __init__(self):
        self._forecast_window = 10
        self.daily_hist = self._load_history()

    def _load_history(self):
        return pd.read_csv(self.HIST_DATA)

    def _convert_to_prophet_df(self, df):
        df_proph = df[['visits', 'date_day']].copy()
        df_proph['y'] = df_proph['visits']
        df_proph['ds'] = df_proph['date_day']
        df_proph = df_proph.drop(['visits', 'date_day'], axis=1)
        return df_proph.sort_values(['ds'])

    def _time_series_prediction(self):
        df = self.daily_hist
        train_results = []
        test_results = []
        for counterId in df['counterId'].unique():
            trail_df = df[df['counterId'] == counterId].copy()
            prophet_df = self._convert_to_prophet_df(trail_df)
            m = Prophet(yearly_seasonality=True, daily_seasonality=True)
            m.fit(prophet_df)
            future = m.make_future_dataframe(periods=self._forecast_window, include_history=True)
            forecast = m.predict(future)
            trail_df['yhat'] = forecast[['yhat']].iloc[:len(trail_df)]
            test_df = forecast[['ds', 'yhat']].iloc[len(trail_df):]
            test_df['counterId'] = counterId

            train_results.append(trail_df)
            test_results.append(test_df)
        if len(train_results) > 0:
            return pd.concat(train_results), pd.concat(test_results)
        return pd.concat(test_results)

    def _feature_ingen(self, df):
        features = df.drop(['parkCode', 'counterIdPave', 'date_day', 'year', 'counterId',
                          'Track_name[starts nearby]', 'Difficulty', 'Length[km]',
                          'Duration[min]', 'Fireplace', 'Shelter', 'Toilet', 'InfoHut', 'DrinkingWater',
                          'Recycling', 'Parking', 'Restaurant', 'BusStop'], axis=1)
        features['holiday'] = features['holiday'].astype(int)
        return features.fillna(0)

    def _form_test_features(self):
        pass

    def _gbm_prediction(self, train_features, test_features):
        y_train = train_features['visits']
        X_train = train_features.drop(['visits'], axis=1)
        gbm = lgb.LGBMRegressor(num_leaves=9, learning_rate=0.05, n_estimators=7)
        gbm.fit(X_train, y_train)
        test_features['visits_pred'] = gbm.predict(test_features)
        return test_features

    def _visits_prediction(self, train_pred, test_pred):
        results = []
        for counterId in train_pred['counterId'].unique():
            trail_df = train_pred[train_pred['counterId'] == counterId]
            train_features = self._feature_ingen(trail_df)
            test_features = self._feature_ingen(test_pred)
            test_features = self._form_test_features()
            test_features = self._gbm_prediction(train_features, test_features)
            results.append(test_features)
        return pd.merge(results)

    def compute_prediction(self):
        train_pred, test_pred = self._time_series_prediction()
        test_pred['visits_pred'] = test_pred['yhat']

        test_pred['visits_pred'] = np.where(test_pred['visits_pred'] < 0, 0, test_pred['visits_pred'])
        test_pred['date'] = test_pred['ds']

        stats = self.daily_hist[['counterId', 'visits']].groupby('counterId').agg('max').reset_index()
        stats['visits_max'] = stats['visits']
        stats = stats.drop(['visits'], axis=1)
        test_pred = test_pred.merge(stats, on='counterId')
        test_pred['load'] = test_pred['visits_pred'] / (test_pred['visits_max'] + 1)

        # test_pred = self._visits_prediction(train_pred, test_pred)
        # test_pred['date'] = test_pred['ds']
        return test_pred[['counterId', 'date', 'visits_pred', 'load']]

    def process_day_parts(self):
        df = pd.read_csv(self.HIST_HOUR)
        df['date'] = df['startDate'].apply(lambda x: datetime.fromtimestamp(x))
        df['year'] = df['date'].apply(lambda x: x.year)
        df['month'] = df['date'].apply(lambda x: x.month)
        df['day'] = df['date'].apply(lambda x: x.day)
        df['hour'] = df['date'].apply(lambda x: x.hour)
        df['date_day'] = df['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        def convert_to_day_part(hour):
            if hour <= 12:
                return 0
            elif hour <= 16:
                return 1
            return 2

        df_processed = df[(df['hour'] >= 9) & (df['hour'] <= 20)]
        df_processed['day_part'] = df['hour'].apply(convert_to_day_part)

        df_processed = df_processed.groupby(['counterId', 'date_day', 'year', 'month', 'day_part']).agg({
            'visits': 'sum',
            'cloud': 'mean',
            'pressure': 'mean',
            'humidity': 'mean',
            'percip': 'mean',
            'show_depth': 'mean',
            'temp': 'mean',
            'dewpoint': 'mean',
            'visib': 'mean',
            'wind_dir': 'mean',
            'gust_speed': 'mean',
            'wind_speed': 'mean',
            'coordSouth': 'mean',
            'holiday': 'max'
        }).reset_index().sort_values(['date_day', 'day_part'])

        df_processed['hours'] = np.where(df_processed['day_part'] == 0, '01:00',
                                np.where(df_processed['day_part'] == 1, '09:00',
                                np.where(df_processed['day_part'] == 2, '17:00', df_processed['day_part'])))
        df_processed['ds'] = df_processed['date_day'] + " " + df_processed['hours']
        return df_processed

    def compute_prediction_day_part(self):
        df = self.process_day_parts()

        def form_dt():
            cur = datetime.now()
            dates = []
            for _ in range(self._forecast_window):
                next = cur + timedelta(days=1)
                dates += [cur.strftime("%Y-%m-%d") + ' ' + t for t in ['01:00', '09:00', '17:00']]
                cur = next
            return pd.DataFrame({'ds': dates})

        train_results = []
        test_results = []
        for counterId in df['counterId'].unique():
            trail_df = df[df['counterId'] == counterId].copy()
            trail_df = trail_df.iloc[-1500:]
            prophet_df = df[['visits', 'ds']].copy()
            prophet_df['y'] = prophet_df['visits']
            prophet_df = prophet_df.sort_values('ds')

            m = Prophet(yearly_seasonality=True, daily_seasonality=True)
            m.fit(prophet_df)

            future = form_dt()
            forecast = m.predict(future)
            # trail_df['yhat'] = forecast[['yhat']].iloc[:len(trail_df)]
            test_df = forecast[['ds', 'yhat']]
            test_df['counterId'] = counterId

            # train_results.append(trail_df)
            test_results.append(test_df)
        if len(train_results) > 0:
            return pd.concat(train_results), pd.concat(test_results)
        return pd.concat(test_results)

    def run(self):
        pred = self.compute_prediction()
        pred.to_csv(self.OUTP_DATA, index=False)

    def run_day_part(self):
        test_pred = self.compute_prediction_day_part()
        test_pred['visits_pred'] = test_pred['yhat']

        test_pred['visits_pred'] = np.where(test_pred['visits_pred'] < 0, 0, test_pred['visits_pred'])
        test_pred['date'] = test_pred['ds']

        stats = self.daily_hist[['counterId', 'visits']].groupby('counterId').agg('max').reset_index()
        stats['visits_max'] = stats['visits']
        stats = stats.drop(['visits'], axis=1)
        test_pred = test_pred.merge(stats, on='counterId')
        test_pred['load'] = test_pred['visits_pred'] / (test_pred['visits_max'] + 1)

        pred = test_pred[['counterId', 'date', 'visits_pred', 'load']]
        pred.to_csv(self.OUTP_DATA2, index=False)


def get_historical_data():
    df = pd.read_csv('result_holiday.csv')


def make_daily_prediction():
    # BusynessPredictor().run()
    BusynessPredictor().run_day_part()


if __name__ == '__main__':
    make_daily_prediction()
