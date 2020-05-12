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

    #20200107追加
    #月、曜日、日を追加
#        stock_data['W'] = pd.to_datetime(stock_data.index, infer_datetime_format=True).dayofweek
#        stock_data['D'] = pd.to_datetime(stock_data.index, infer_datetime_format=True).day
#        stock_data['M'] = pd.to_datetime(stock_data.index, infer_datetime_format=True).month

    #20200109追加
    stock_data = stock_data.replace([np.inf, -np.inf], np.nan)
    stock_data = stock_data.fillna(0)
    return stock_data

def fx_data(df_info):  #●サブデータ
    #nowを日付に置換(yyyy:mm:dd hh:mm:ss → yyyy-mm-dd)
#        df_info['now'] = df_info['now'].map(lambda x: x[:10].replace('/', '-'))
    #インデックス再設定 日時処理のみ
    df_info = df_info.set_index('now')
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
def create_y( code, df_info,rang = -1):
    # 翌日終値 - 当日終値で差分を計算
    #shift(-1)でcloseを上に1つずらす
    df = df_info
    df['close+1'] = df[code].shift(rang).astype(np.float64) / df[code].astype(np.float64) -1
    df['diff'] = df['close+1'].apply(lambda x: -1 if x < 0 else 1)
    """
    for i in range(len(df['diff'])):
        if df['diff'][i] > 0:
            df['diff'][i] = 1
        elif df['diff'][i] < 0:
            df['diff'][i] = -1
        else:
            df['diff'][i] = 0
    """
    #最終日はclose+1がNaNになるので削る
    df = df[:-1]
    # 不要なカラムを削除
    del df['close+1']

    # 上昇と下降のデータ割合を確認
    m = len(df[code])
    #df['diff']>0で全行に対してtrueかfalseで返してくれる。df[(df['diff'] > 0)]で
    #dff>0に絞って全てのカラムを出力
    print(">0",len(df[(df['diff'] > 0)]) / m * 100)
    print("<0",len(df[(df['diff'] < 0)]) / m * 100)
    print("=0",len(df[(df['diff'] == 0)]) / m * 100)
    return df

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
def data_exec2( x_data, y_data, Change=2, hist_rang=1):  #何日分使うか決める nday

    # 各列を変化率へ一括変換
    x_data = RateOfChange(x_data, Change, hist_rang)  # 第二引数 整数2の場合おおよそ-1.0～1.0の範囲 第三引数 何日まえからの変化率
    #x_data = self.RateOfChange(x_data,'MinMax',hist_rang) #正規化
    #x_data = self.RateOfChange(x_data,'Standard',hist_rang) #標準化

    X_train, X_test, y_train, y_test =train_test_split(x_data, y_data, train_size=0.8,test_size=0.1,random_state=1)
    # 決定技モデルの訓練
    clf_2 = DecisionTreeClassifier(max_depth=5)

    # grid searchでmax_depthの最適なパラメータを決める
    #k=10のk分割交差検証も行う
    params = {'max_depth': [2, 5, 10, 20]}

    grid = GridSearchCV(estimator=clf_2,
                        param_grid=params,
                        cv=10,
                        scoring='roc_auc')
    grid.fit(X_train, y_train)
    for r, _ in enumerate(grid.cv_results_['mean_test_score']):
        print("%0.3f +/- %0.2f %r"
            % (grid.cv_results_['mean_test_score'][r],
                grid.cv_results_['std_test_score'][r] / 2.0,
                grid.cv_results_['params'][r]))
    print('Best parameters: %s' % grid.best_params_)
    print('Accuracy: %.2f' % grid.best_score_)

    #grid searchで最適だったパラメータを使って学習する
    clf_2 = grid.best_estimator_
    clf_2 = clf_2.fit(X_train, y_train)
    #モデルを保存する。
    filename = os.path.join(self.S_DIR, self.code.replace("/", "") + "_" + str(self.num) + "_" + str(Change) + "_" + '_model.sav')
#        joblib.dump(clf_2, filename)
    pickle.dump(clf_2, open(filename, 'wb'))
    pred_test_2 = clf_2.predict(X_test)
    """
    #重要度の高い素性を表示
    importances = clf_2.feature_importances_
    indices = np.argsort(importances)[::-1]

    for f in range(X_train.shape[1]):
        print("%2d) %-*s %f" % (f + 1, 30,
                                x_data.columns[1+indices[f]],
                                importances[indices[f]]))
    """
    #テストデータ 正解率
#        print(accuracy_score(y_test, pred_test_2))
#        print(classification_report(y_test, pred_test_2))
    addRow = pd.Series([info.code,
                        self.num,
                        Change,
                        grid.best_score_,
                        accuracy_score(y_test, pred_test_2)],
                        index=self.df.columns)
    self.df = self.df.append(addRow, ignore_index=True)
    filename = os.path.join(self.S_DIR , "MODEL_SCORE.csv")
    self.df.to_csv(filename,encoding = 'cp932')

    return accuracy_score(y_test, pred_test_2)


if __name__ == "__main__":
    sql_pd = common.select_sql(r'I07_fx.sqlite', 'select * from %(table)s where rowid > (select max(rowid) from %(table)s)  - %(key1)s' % {'table': 'gmofx', 'key1': 1000000})
    del sql_pd['uptime']
    del sql_pd['result']
    df_info = fx_data(sql_pd) #いらないデータやNoneの補修など実施

    for code in sql_pd.columns[1:20]:
        df = create_y(code, df_info, -1)  #最後の引数は何日後の予測をするか？
        x_data = add_avg(df, code)
#        for nday in range(100,5000,200):
