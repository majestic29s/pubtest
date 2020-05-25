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
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.externals import joblib
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import accuracy_score, classification_report
# 機械学習ライブラリ
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
import pickle
import datetime, os, shutil, sys

import common
sys.path.append(common.LABO_DIR)
import common_sklearn as sk

search_params = {
    'n_estimators' : [10, 20, 30, 40, 50, 100, 200, 300],
    'max_features': [None, 'auto', 'log2'],
    'max_depth': [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
    'class_weight': [None, 'balanced'],
    'random_state' : [42]
    }

class scikit_learn:
    def __init__(self, num):
        self.num = num
        self.code = ""
        self.Change = ""
        self.haba = ""
        self.report = {}
        self.df = pd.DataFrame([], columns=["code","recall_score" ,"precision_score" ,"tran", "pred","title","Change","haba"])
        t = datetime.datetime.now()
        self.date = t.strftime("%Y%m%d%H%M%S")
        #保存フォルダルート
        self.S_DIR = os.path.join(r"C:\data\90_profit\06_output", num, self.date)
        os.mkdir(str(self.S_DIR))
        #本スクリプトコピー
        shutil.copy2(__file__, self.S_DIR)

    def model1(self, code, x_data, y_data):
        X = np.array(x_data)  # numpy配列に変換する。
        Y = np.array(y_data)
        Y = np.reshape(Y, (Y.shape[0], 1))  # 3次元配列に変換する。
        X = np.reshape(X, (X.shape[0], 1, X.shape[1]))  # 3次元配列に変換する。
        # train, testデータを定義
        X_train, X_test, Y_train, Y_test = train_test_split(X, Y, train_size=0.8, shuffle=False)
        row = int(X.shape[0] * 0.8)
        IDX = y_data[row:]

        print(X_test.shape) #(11604, 1, 34)
        print(Y_test.shape)  #(11603, 1)

        model = Sequential()
        model.add(LSTM(10, activation = 'tanh', input_shape = (1,X_test.shape[2]), recurrent_activation= 'hard_sigmoid'))
        model.add(Dense(1))
        model.compile(loss= 'mean_squared_error', optimizer = 'rmsprop', metrics=[metrics.mae])
        model.fit(X_train, Y_train, epochs=10, verbose=2)
        Predict = model.predict(X_test)

        #pandasに変更・結合
        idx = pd.DataFrame(list(IDX.index))
        Predict = pd.DataFrame(Predict)
        Y_test = pd.DataFrame(Y_test)
        df = pd.concat([idx, Predict, Y_test], axis=1)
        df.columns = ['idx','Pred','Y_test'][: len(df.columns)]
#        df = pd.DataFrame({'Pred':Predict, 'Y_test':Y_test}, index=IDX.index)
        print(df)
        df.to_csv("result.csv")
        exit()

        N = len(df)
        L_PL = np.zeros(N)
        L_SUM = np.zeros(N)
        S_PL = np.zeros(N)
        S_SUM = np.zeros(N)
        for i in range(2, N):
            L_SUM[i] = L_SUM[i - 1]
            S_SUM[i] = S_SUM[i - 1]
            if df.Pred[i] - df.Pred[i - 1] > 0:
                L_PL[i] = df.Y_test[i] - df.Y_test[i - 1]
                L_SUM[i] = L_SUM[i] + L_PL[i]
            elif df.Pred[i] - df.Pred[i - 1] < 0:
                S_PL[i] = df.Y_test[i - 1] - df.Y_test[i]
                S_SUM[i] = S_SUM[i] + S_PL[i]
        df1 = pd.DataFrame({'L_PL':L_PL,'S_PL':S_PL, 'L_SUM':L_SUM,'S_SUM':S_SUM}, index=df.index)
        df = pd.concat([df,df1], axis=1)
        df.to_csv("test1.csv")
        exit()
    #    Predict.index=pd.to_datetime(df.iloc[193:,0])
        plt.figure(figsize=(15,10))
        plt.plot(Y_test, label = 'Test')
        plt.plot(Predict, label = 'Prediction')
        plt.legend(loc='best')
        plt.show()

    def model2(self, code, x_data, y_data):
        X_train, X_test, Y_train, Y_test = train_test_split(x_data, y_data, train_size=0.8, shuffle=False)

        #学習宇モデルの構築
        print('学習宇モデルの構築')
        forest = RandomForestClassifier()
        grid = GridSearchCV(forest, search_params, cv=3)
        grid.fit(X_train, Y_train)

        #学習結果表示
        print("Grid-Search with accuray")
        print("Best parameters:", grid.best_params_)
        print("Test set score {:.2f} ".format(grid.score(X_test, Y_test)))

        filename = os.path.join(self.S_DIR, code.replace("/", "") + "_" + '_model2.pkl')
        print(filename)
        joblib.dump(grid.best_estimator_,filename)

        print(str(code) + " : モデル保存完了")

    def model3(self, code, x_data, y_data, title):  #何日分使うか決める nday
        print(x_data.shape, y_data.shape)
        X_train, X_test, y_train, y_test = train_test_split(x_data, y_data, train_size=0.8, shuffle=False)
        # 決定技モデルの訓練
        clf_2 = DecisionTreeClassifier(max_depth=5)
        # grid searchでmax_depthの最適なパラメータを決める
        #k=10のk分割交差検証も行う
        params = {'max_depth': [2, 5, 10, 20],'class_weight': ['balanced']}

        grid = GridSearchCV(estimator=clf_2,
                            param_grid=params,
                            cv=10,
                            scoring='accuracy')
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
        filename = os.path.join(self.S_DIR, code.replace("/", "") + "_" +  title + "_" + str(self.Change) + "_" + str(self.haba) + "_" + '_model3.sav')
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
        print(classification_report(y_test, pred_test_2))
        fit_ = accuracy_score(y_test, pred_test_2)
        addRow = pd.Series([code,
                            recall_score(y_test, pred_test_2),
                            precision_score(y_test, pred_test_2),
                            grid.best_score_,
                            fit_,
                            title,
                            self.Change,
                            self.haba],
                            index=self.df.columns)
        self.df = self.df.append(addRow, ignore_index=True)
        filename = os.path.join(self.S_DIR , "MODEL_SCORE.csv")
        self.df.to_csv(filename,encoding = 'cp932')
        print(fit_)
        print(recall_score(y_test, pred_test_2))
        print(precision_score(y_test, pred_test_2))
        return

if __name__ == "__main__":
    info = scikit_learn('model')
    df_info = sk.fx_data()  #いらないデータやNoneの補修など実施
    for code in ['USD/JPY', 'EUR/USD', 'EUR/JPY', 'GBP/JPY','AUD/JPY']:
        x_data_ = sk.add_avg(df_info.copy(), code)
        info.haba = 0.001
#        info.haba = 0
        x_data, y_plus = sk.create_y(code, x_data_.copy(), -4, info.haba)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
#        x_mynus, y_mynus = sk.create_y(code, x_data_.copy(), -4, -info.haba)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
#        x_data, y_data = sk.create_y(code, x_data_.copy(), -4, 9999)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
#        x_data, y_data = sk.create_y(code, x_data_.copy(), -4, info.haba)  #3引数は何日後の予測をするか？ 4引数は目的変数 0は1or-1 9999は株価
#        for Change in [2,"MinMax","Standard"]:
        for Change in ["MinMax"]:
            info.Change = Change
            x_data = sk.RateOfChange(x_data, info.Change, 1)
            print(code, "学習スタート")
#            info.model1(code, x_data, y_data) #実値
#            info.model2(code, x_data, y_data)  # 1 or -1 処理時間が長い
#            info.model3(code, x_data, y_data,"y_data")  # 1 or -1
            info.model3(code, x_data, y_plus,"plus")  # 1 or -1
#            info.model3(code, x_mynus, y_mynus,"mynus")  # 1 or -1
