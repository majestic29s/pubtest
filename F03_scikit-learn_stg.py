#!/usr/bin/env python
# -*- coding: utf-8 -*-
import warnings
warnings.filterwarnings('ignore')  # 実行上問題ない注意は非表示にする

# Pandasのimport
import pandas as pd
import numpy as np
import sys, os
import common


#モデルの保存・読み込み
from sklearn.externals import joblib
#import sklearn
import joblib
import pickle

sys.path.append(common.LABO_DIR)
import _common_sklearn as sk

class scikit_learn:
    def __init__(self,num):
        self.num = num
        self.code = ""

    def main(self):
        df = sk.fx_data(100)
        for code in ['USD/JPY','EUR/USD','EUR/JPY','GBP/JPY']:
            x_data = sk.add_avg(df.copy(), code)
            filename = os.path.join(common.MODEL, code.replace("/", "") + '_finalized_model.sav')
            clf_2 = joblib.load(filename)

            pred_test_2 = clf_2.predict(x_data)
            print(pred_test_2[-1])
            sqls = common.create_update_sql('I07_fx.sqlite', {code.replace("/","") + '_result':pred_test_2[-1]}, 'gmofx')


if __name__ == "__main__":
    info = scikit_learn(0)
    info.main()
