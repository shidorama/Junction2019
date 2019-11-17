import numpy as np
import pandas as pd
from datetime import datetime
import seaborn as sns
import matplotlib.pyplot as plt
from pylab import rcParams
import time
rcParams['figure.figsize'] = 12, 5


def prepare_df():
    df = pd.read_csv('data/history_hour.csv')
    df['date'] = df['startDate'].apply(lambda x: datetime.fromtimestamp(x))
    df['hour'] = df['date'].apply(lambda x: x.hour)
    df['hour2d'] = df['hour'] // 2 * 2
    df['weekday'] = df['date'].apply(lambda x: x.weekday())
    return df[['parkCode', 'counterId', 'weekday', 'hour2d', 'visits']]


def dump_img(df, fieldValue, weekday):
    path = 'data/hists/{}_{}.png'.format(fieldValue, weekday)
    hours = df['hour2d'].values
    visits = df['visits'].values

    plt.clf()
    plt.xlabel('hour')
    plt.ylabel('visits')
    plt.bar(hours, visits, width=1.8)
    plt.xlim(xmin=8, xmax=22)
    plt.xticks(np.arange(8, 20, 2))
    plt.savefig(path)


def build_hists():
    field = 'parkCode'
    # field = 'counterId'
    df = prepare_df()
    df = df.groupby([field, 'weekday', 'hour2d']).agg('mean').reset_index().sort_values('hour2d')
    for fieldValue in df[field].unique():
        print('fieldValue:', fieldValue)
        df_split = df[df[field] == fieldValue]
        for weekday in df_split['weekday'].unique():
            df_split2 = df_split[df_split['weekday'] == weekday]
            dump_img(df_split2, fieldValue, weekday)


if __name__ == '__main__':
    build_hists()
