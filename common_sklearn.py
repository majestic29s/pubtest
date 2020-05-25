#!/usr/bin/env python
# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')  # 実行上問題ない注意は非表示にする

# サポートベクターマシーンのimport
from sklearn import svm
# train_test_splitのimport
from sklearn.model_selection import train_test_split
# Pandasのimport
import pandas as pd
import numpy as np
# グリッドサーチのimport
from sklearn.model_selection import GridSearchCV
# 機械学習ライブラリ
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import accuracy_score,classification_report
#モデルの保存・読み込み
from sklearn.externals import joblib
import pickle

import common_profit as compf
import os, csv, sys, datetime
import shutil
import common
sys.path.append(common.LIB_DIR)



def RateOfChange( stock_data, haba, cnt=1):
    #変化率、正規化、標準化の3つに対応
    #パラメータ
    # stock_dataデータ
    # haba 2の場合おおよそ-1.0～1.0の範囲に収まるように調整
    # cnt 何日まえからの変化率 1は昨日からの変化率
    #全データの変化率 0～1
    if type(haba) == int:
        #文字列の列は削除する。
        for column in stock_data.columns:
            if stock_data[column].dtypes != 'int64' and stock_data[column].dtypes != 'float64':
                stock_data.drop([column], axis=1, inplace=True)
#                    print("delete_column_" + column)
        stock_data = stock_data.pct_change(cnt) * haba
    else:
        class_type = haba
        if class_type == "MinMax":#正規化
            #前処理としてMinMaxScalerを用いる。まずはMinMaxScalerのインスタンスを作る。
            from sklearn.preprocessing import MinMaxScaler
            #パラメータcopyのTureは別オブジェクトとして
            MinMaxScaler(copy=True, feature_range=(0, 1))
            instance = MinMaxScaler() # インスタンスの作成
        elif  class_type == "Standard":#標準化
            from sklearn.preprocessing import StandardScaler
            #パラメータcopyのTureは別オブジェクトとして
            StandardScaler(copy=True, with_mean=True, with_std=True)
            instance = StandardScaler()  # インスタンスの作成

        for column in stock_data.columns:
            x = stock_data[[column]].values
            try:
                x_scaled = instance.fit_transform(x)
                stock_data[column] = x_scaled
            except:
                print("ERROR",stock_data[column].dtypes)
                #labelsは数字、uniquesは文字列、全列を抽出
                labels, uniques = pd.factorize(stock_data[column])
                #新たにラベルを追加し、変換した整数を追加する。
                stock_data[column] = labels

    #20200109追加
    stock_data = stock_data.replace([np.inf, -np.inf], np.nan)
    stock_data = stock_data.fillna(0)
    return stock_data

def fx_data(rows= 1000000):  #●サブデータ
    sql_pd = common.select_sql(r'I07_fx.sqlite', 'select * from %(table)s where rowid > (select max(rowid) from %(table)s)  - %(key1)s' % {'table': 'gmofx', 'key1': rows})
    df_info = sql_pd.iloc[:, :20]
    #nowを日付に置換(yyyy:mm:dd hh:mm:ss → yyyy-mm-dd)
#        df_info['now'] = df_info['now'].map(lambda x: x[:10].replace('/', '-'))
    #インデックス再設定 日時処理のみ
    df_info = df_info.set_index('now')
    df_info['M'] = pd.to_datetime(df_info.index, infer_datetime_format=True).month
    df_info['W'] = pd.to_datetime(df_info.index, infer_datetime_format=True).dayofweek
    df_info['H'] = pd.to_datetime(df_info.index, infer_datetime_format=True).hour
    #NaNを置換
    df_info = df_info.fillna(0)
    #数字以外は置換
    df_info = df_info.replace(['nan', '--', '-'], 0)
    list_w = []
    for col in df_info.columns:
        try:
            df_info[col] = df_info[col].map(lambda x: str(x).replace(',', ''))
            df_info[col] = df_info[col].astype(np.float64)
            #20200106追加
            ZERO = sum(df_info[col] == 0) / len(df_info)
            #全体で50%以上は削除
            if ZERO > 0.5:
                print("del",col,ZERO)
                list_w.append(col)
        except:
            print("NG",col)
            list_w.append(col)

    df_info = df_info.drop(list_w, axis=1)

    return df_info
def create_y( code, df_info,rang = -1,res = 0):
    # 翌日終値 - 当日終値で差分を計算
    #shift(-1)でcloseを上に1つずらす
    df = df_info
    df['diff'] = df[code].shift(rang).astype(np.float64) / df[code].astype(np.float64) - 1
#    df = df.dropna(subset=['diff'])  #diffに欠損値のある行を削除
    if res == 9999:
        df['diff'] = df[code].shift(rang).astype(np.float64)
    elif res is not None:
        if res > 0:
            df['diff'] = df['diff'].apply(lambda x: 1 if x > res else -1)
        else:
            df['diff'] = df['diff'].apply(lambda x: 1 if x < res else - 1)
#        df['diff1'] = df['diff'].apply(lambda x: 1 if x > res else -1  if x < res * -1 else 0 )

    df = df.dropna(subset=['diff'])  #diffに欠損値のある行を削除
    sp = df.shape[1] - 1
    X = df.iloc[:, :sp]
    y = df.iloc[:, sp:]
    #数の偏りを修正する 1=1にする。
    if 1 > res > 0 or -1 < res < 0:
        # データを一旦分別
        X_0 = X[y['diff']<=res]
        X_1 = X[y['diff']>res]
        y_0 = y[y['diff']<=res]
        y_1 = y[y['diff']>res]
        # X_0をX_1とほぼ同じ数にする
        X_dummy, X_t, y_dummy, y_t = train_test_split(X_0, y_0, test_size=0.09, random_state=0)
        # 分別したデータの結合
#        X = pd.concat([X_1, X_t], axis=0)
        X = pd.concat([X_1, X_t])
        y = pd.concat([y_1, y_t])
        print(X.shape,y.shape)

    # 上昇と下降のデータ割合を確認
    m = len(y)
    print(">0:", round(len(y[(y['diff'] > 0)]) / m * 100, 2), "\t<0:", round(len(y[(y['diff'] < 0)]) / m * 100, 2))

    return X, y

def add_avg( df, code):  #●最後の引数は過去データ数
    #移動平均の計算、5日、25日、50日、75日
    #ついでにstdも計算する。（=ボリンジャーバンドと同等の情報を持ってる）
    #75日分のデータ確保
    nclose = len(df.columns)
    for i in range(1, 75):
        df[str(i)] = df[code].shift(+i)
    #移動平均の値とstdを計算する, skipnaの設定で一つでもNanがあるやつはNanを返すようにする
    df[code + 'MA5'] = df.iloc[:, np.arange(nclose, nclose+5)].mean(axis='columns', skipna=False)
    df[code + 'MA25'] = df.iloc[:, np.arange(nclose, nclose+25)].mean(axis='columns', skipna=False)
    df[code + 'MA50'] = df.iloc[:, np.arange(nclose, nclose+50)].mean(axis='columns', skipna=False)
    df[code + 'MA75'] = df.iloc[:, np.arange(nclose, nclose+75)].mean(axis='columns', skipna=False)

    df[code + 'STD5'] = df.iloc[:, np.arange(nclose, nclose+5)].std(axis='columns', skipna=False)
    df[code + 'STD25'] = df.iloc[:, np.arange(nclose, nclose+25)].std(axis='columns', skipna=False)
    df[code + 'STD50'] = df.iloc[:, np.arange(nclose, nclose+50)].std(axis='columns', skipna=False)
    df[code + 'STD75'] = df.iloc[:, np.arange(nclose, nclose+75)].std(axis='columns', skipna=False)
    #計算終わったら余分な列は削除
    for i in range(1, 75):
        del df[str(i)]
    #それぞれの平均線の前日からの変化（移動平均線が上向か、下向きかわかる）
    #shift(-1)でcloseを上に1つずらす
    df[code + 'diff_MA5'] = df[code + 'MA5'] - df[code + "MA5"].shift(1)
    df[code + 'diff_MA25'] = df[code + 'MA25'] - df[code + "MA25"].shift(1)
    df[code + 'diff_MA50'] = df[code + 'MA50'] - df[code + "MA50"].shift(1)
    df[code + 'diff_MA75'] = df[code + 'MA75'] - df[code + "MA75"].shift(1)
    #3日前までのopen, close, high, lowも素性に加えたい
    for i in range(1, 4):
        df['close-'+str(i)] = df[code].shift(+i)
#            df['open-'+str(i)] = df.open.shift(+i)
#            df['high-'+str(i)] = df.high.shift(+i)
#            df['low-'+str(i)] = df.low.shift(+i)
    #NaNを含む行を削除
    df = df.dropna()
    return df

if __name__ == "__main__":
    df_info = fx_data() #いらないデータやNoneの補修など実施
    for code in df_info:
        x_data_ = add_avg(df_info.copy(), code)
#        x_data, y_data = create_y(code, x_data, -4, 0.001)  #最後の引数は何日後の予測をするか？
#        x_data, y_plus = create_y(code, x_data_.copy(), -4, info.haba)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
#        x_data, y_mynus = create_y(code, x_data_.copy(), -4, -info.haba)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
#        x_data, y_data = create_y(code, x_data_.copy(), -4, 9999)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
        x_data, y_data = create_y(code, x_data_.copy(), -4, 0.001)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
        print(y_data)
        exit()
#        for nday in range(100,5000,200):
