#!/usr/bin/env python
# -*- coding: utf-8 -*-
#https://qiita.com/shiroino11111/items/f812938fbbba7123fbcc
#精度６７％のディープラーニング株価予測モデル_1

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from keras.models import Sequential
from keras.layers import Dense,  LSTM
from keras import metrics
from sklearn.preprocessing import MinMaxScaler
import common
import _sklearn
import datetime,os,shutil

class scikit_learn:
    def __init__(self, num):
        self.num = num
        self.code = ""
        self.report = {}
        self.df = pd.DataFrame([], columns=["code","histrange" ,"model" ,"tran", "pred"])
        t = datetime.datetime.now()
        self.date = t.strftime("%Y%m%d%H%M%S")
        #保存フォルダルート
        self.S_DIR = os.path.join(r"C:\data\90_profit\06_output", num, self.date)
        os.mkdir(str(self.S_DIR))
        #本スクリプトコピー
        shutil.copy2(__file__, self.S_DIR)

    def main(self, x_data, code):
        YY = x_data[code].shift(1).astype(np.float64) / x_data[code].astype(np.float64) - 1
        x_data = _sklearn.RateOfChange(x_data,'MinMax',1)

        X = np.array(x_data)  # numpy配列に変換する。
        X = X[1: X.shape[0],]
        Y = np.array(YY[1:])
        Y = Y[1: Y.shape[0],]

#        X = np.reshape(X, (X.shape[0], 1, X.shape[1]))  # 3次元配列に変換する。
        X = np.reshape(X, (X.shape[0], X.shape[1]))  # 3次元配列に変換する。
        Y = np.reshape(Y, (Y.shape[0], 1))  # 3次元配列に変換する。
        # train, testデータを定義
        row = int(X.shape[0] * 0.8)
#        X_train = X[:row, :, :]
#        X_test = X[row:, :, :]
        X_train = X[:row, :]
        X_test = X[row:, :]
#        Y_train = Y[:row, :]
#        Y_test = Y[row:, :]
        Y_train = Y[:row]
        Y_test = Y[row:]
        model = Sequential()
        model.add(LSTM(10, activation = 'tanh', input_shape = (1,3), recurrent_activation= 'hard_sigmoid'))
        model.add(Dense(1))
        model.compile(loss= 'mean_squared_error', optimizer = 'rmsprop', metrics=[metrics.mae])
        model.fit(X_train, Y_train, epochs=10, verbose=2)
        Predict = model.predict(X_test)

        print(accuracy_score(y_test, Predict))
        exit()
        # オリジナルのスケールに戻す、タイムインデックスを付ける。
        Y_train = scaler1.inverse_transform(Y_train)
        Y_train = pd.DataFrame(Y_train)
        print(Y_train)
    #    Y_train.index = pd.to_datetime(df.iloc[3:row+3,0])
        Y_test = scaler1.inverse_transform(Y_test)
        Y_test = pd.DataFrame(Y_test)
    #    Y_test.index = pd.to_datetime(df.iloc[row+3:,0])
        Predict = scaler1.inverse_transform(Predict)
        Predict = pd.DataFrame(Predict)
    #    Predict.index=pd.to_datetime(df.iloc[193:,0])
        plt.figure(figsize=(15,10))
        plt.plot(Y_test, label = 'Test')
        plt.plot(Predict, label = 'Prediction')
        plt.legend(loc='best')
        plt.show()

    def main2(self):
        df = common.select_sql(r'I07_fx.sqlite', 'select * from %(table)s where rowid > (select max(rowid) from %(table)s)  - %(key1)s' % {'table': 'gmofx', 'key1': 1000000})

        L = len(df)
        Y = df.iloc[:, 4]  # 終値の列のみ抽出する。
        Y = np.array(Y)  # numpy配列に変換する。
        Y = Y.reshape(-1, 1)  # 行列に変換する。（配列の要素数行×1列）
        X1 = Y[0:L-3, :]  # 予測対象日の3日前のデータ
        X2 = Y[1:L-2, :]  # 予測対象日の2日前のデータ
        X3 = Y[2:L-1, :]  # 予測対象日の前日データ
        Y = Y[3:L, :]  # 予測対象日のデータ
        X = np.concatenate([X1, X2, X3], axis=1)  # numpy配列を結合する。
        scaler = MinMaxScaler()  # データを0～1の範囲にスケールするための関数。
        scaler.fit(X)  # スケーリングに使用する最小／最大値を計算する。
        X = scaler.transform(X)  # Xをを0～1の範囲にスケーリングする。
        scaler1 = MinMaxScaler()  # データを0～1の範囲にスケールするための関数。
        scaler1.fit(Y)  # スケーリングに使用する最小／最大値を計算する。
        Y = scaler1.transform(Y)  # Yをを0～1の範囲にスケーリングする。
        X = np.reshape(X, (X.shape[0], 1, X.shape[1]))  # 3次元配列に変換する。
        # train, testデータを定義
        row = int(X.shape[0] * 0.8)
        X_train = X[:row, :, :]
        X_test = X[row:, :, :]
        Y_train = Y[:row, :]
        Y_test = Y[row:, :]
        model = Sequential()
        model.add(LSTM(10, activation = 'tanh', input_shape = (1,3), recurrent_activation= 'hard_sigmoid'))
        model.add(Dense(1))
        model.compile(loss= 'mean_squared_error', optimizer = 'rmsprop', metrics=[metrics.mae])
        model.fit(X_train, Y_train, epochs=100, verbose=2)
        Predict = model.predict(X_test)

if __name__ == "__main__":
    info = scikit_learn('model')
    info.main2()
    exit()
    sql_pd = common.select_sql(r'I07_fx.sqlite', 'select * from %(table)s where rowid > (select max(rowid) from %(table)s)  - %(key1)s' % {'table': 'gmofx', 'key1': 1000000})
    del sql_pd['uptime']
    del sql_pd['result']
    df_info = _sklearn.fx_data(sql_pd) #いらないデータやNoneの補修など実施
    code = 'USD/JPY'
    x_data = _sklearn.add_avg(df_info, code)
    info.main(x_data,code)


