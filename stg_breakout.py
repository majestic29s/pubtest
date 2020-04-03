import pandas as pd
import os
import sys
from selenium import webdriver
from bs4 import BeautifulSoup
import re
import numpy as np

import common
sys.path.append(common.LIB_DIR)
import f02_gmo
import f03_ctfx
import datetime


# クロスオーバー移動平均
def ma_two(window0, window9, tablename, sql_pd, DB):
    # 過去データよりデータ加工
    data = {}
    sqls = "select *,rowid from " + tablename
    cnt = len(sql_pd) - 1
    data['avg_'+str(window0)] = sql_pd['S3_R'].rolling(window0).mean()[cnt]
    data['avg_'+str(window9)] = sql_pd['S3_R'].rolling(window9).mean()[cnt]
    data['S3_R'] = float(sql_pd.iloc[-1]['S3_R'])

    # 前回データよりデータ取得 status and price
    tsd = common.select_sql(DB, sqls)
    try:
        data['status'] = int(tsd.iloc[-1]['status'])
        data['price'] = float(tsd.iloc[-1]['price'])
    except:
        data['status'] = 0
        data['price'] = 0
    common.to_number(data)

    #仕切り戦略
    if data['avg_'+str(window0)] > data['avg_'+str(window9)] and data['status'] < 0:  # exit short-position
        data['S_PL'] = data['price']-data['S3_R']
        data['price'] = ""
        data['status'] = 0
        status = -2
    elif data['avg_'+str(window0)] < data['avg_'+str(window9)] and data['status'] > 0:  # exit short-position
        data['L_PL'] = data['S3_R']-data['price']
        data['price'] = ""
        data['status'] = 0
        status = 2
    #仕掛け戦略
    elif data['avg_'+str(window0)] < data['avg_'+str(window9)] and data['status'] == 0:  # entry short-position
        data['price'] = data['S3_R']
        data['status'] = -1
        status = -1
    elif data['avg_'+str(window0)] > data['avg_'+str(window9)] and data['status'] == 0:  # entry short-position
        data['price'] = data['S3_R']
        data['status'] = 1
        status = 1
    else:
        status = 0
    #データ書き込み
    common.insertDB3(DB, tablename, data)
    return status


# 3つの移動平均を使った戦略
def ma_three(window0, window5, window9, tablename, sql_pd, DB):
    # 過去データよりデータ加工
    data = {}
    cnt = len(sql_pd) - 1
    data['avg_' + str(window0)] = sql_pd['S3_R'].rolling(window0).mean()[cnt]
    data['avg_' + str(window5)] = sql_pd['S3_R'].rolling(window5).mean()[cnt]
    data['avg_' + str(window9)] = sql_pd['S3_R'].rolling(window9).mean()[cnt]
    data['S3_R'] = float(sql_pd['S3_R'][cnt])

    # 前回データよりデータ取得 status and price
    sqls = "select *,rowid from %(table)s" % {'table': tablename}
    tsd = common.select_sql(DB, sqls)
    try:
        data['status'] = int(tsd.iloc[-1]['status'])
        data['price'] = float(tsd.iloc[-1]['price'])
    except:
        data['status'] = 0
        data['price'] = 0
    common.to_number(data)

    #仕切り戦略
    if (data['avg_'+str(window0)] > data['avg_'+str(window5)] or data['avg_'+str(window5)] > data['avg_'+str(window9)]) and data['status'] < 0:
        data['S_PL'] = data['price']-data['S3_R']
        data['price'] == ""
        data['status'] = 0
        status = -2
    elif (data['avg_'+str(window0)] < data['avg_'+str(window5)] or data['avg_'+str(window5)] < data['avg_'+str(window9)]) and data['status'] > 0:
        data['L_PL'] = data['S3_R']-data['price']
        data['price'] == ""
        data['status'] = 0
        status = 2
    #仕掛け戦略
    elif data['S3_R'] < data['avg_'+str(window0)] < data['avg_'+str(window5)] < data['avg_'+str(window9)] and data['status'] == 0:
        data['price'] = data['S3_R']
        data['status'] = -1
        status = -1
    elif data['S3_R'] > data['avg_'+str(window0)] > data['avg_'+str(window5)] > data['avg_'+str(window9)] and data['status'] == 0:
        data['price'] = data['S3_R']
        data['status'] = 1
        status = 1
    else:
        status = 0
    #データ書き込み
    common.insertDB3(DB, tablename, data)
    return status

# 3つの移動平均を使った戦略
def ma_four(window0, window5, window9, window10, tablename, sql_pd, DB):
    # 過去データよりデータ加工
    data = {}
    cnt = len(sql_pd) - 1
    data['avg_' + str(window0)] = sql_pd['S3_R'].rolling(window0).mean()[cnt]
    data['avg_' + str(window5)] = sql_pd['S3_R'].rolling(window5).mean()[cnt]
    data['avg_' + str(window9)] = sql_pd['S3_R'].rolling(window9).mean()[cnt]
    data['avg_' + str(window10)] = sql_pd['S3_R'].rolling(window10).mean()[cnt]
    data['S3_R'] = float(sql_pd['S3_R'][cnt])
    # 前回データよりデータ取得 status and price
    sqls = "select *,rowid from %(table)s" % {'table': tablename}
    tsd = common.select_sql(DB, sqls)

    try:
        data['status'] = int(tsd.iloc[-1]['status'])
        data['price'] = float(tsd.iloc[-1]['price'])
    except:
        data['status'] = 0
        data['price'] = 0
    common.to_number(data)

    #仕切り戦略
    if (data['avg_'+str(window0)] > data['avg_'+str(window5)] or data['avg_'+str(window5)] > data['avg_'+str(window9)]) and data['status'] < 0:
        data['S_PL'] = data['price']-data['S3_R']
        data['price'] == ""
        data['status'] = 0
        status = -2
    elif (data['avg_'+str(window0)] < data['avg_'+str(window5)] or data['avg_'+str(window5)] < data['avg_'+str(window9)]) and data['status'] > 0:
        data['L_PL'] = data['S3_R']-data['price']
        data['price'] == ""
        data['status'] = 0
        status = 2
    #仕掛け戦略
    elif data['S3_R'] < data['avg_'+str(window0)] < data['avg_'+str(window5)] < data['avg_'+str(window9)] < data['avg_'+str(window10)] and data['status'] == 0:
        data['price'] = data['S3_R']
        data['status'] = -1
        status = -1
    elif data['S3_R'] > data['avg_'+str(window0)] > data['avg_'+str(window5)] > data['avg_'+str(window9)] > data['avg_'+str(window10)] and data['status'] == 0:
        data['price'] = data['S3_R']
        data['status'] = 1
        status = 1
    else:
        status = 0
    #データ書き込み
    common.insertDB3(DB, tablename, data)
    return status

# 過去の高値・安値を用いたブレイクアウト戦略
def simple(window0, window9, tablename, sql_pd, DB):
    # 過去データよりデータ加工
    data = {}
    cnt = len(sql_pd) - 1
    data['ub0'] = sql_pd['S3_R'].rolling(window0).max()[cnt]  #ub0
    data['lb0'] = sql_pd['S3_R'].rolling(window0).min()[cnt] #2
    data['ub9'] = sql_pd['S3_R'].rolling(window9).max()[cnt] #ub9
    data['lb9'] = sql_pd['S3_R'].rolling(window9).min()[cnt]  #lb9
    data['S3_R'] = float(sql_pd['S3_R'][cnt])
    data['ub0l'] = sql_pd['S3_R'].rolling(window0).max()[cnt-1]  #ub0
    data['lb0l'] = sql_pd['S3_R'].rolling(window0).min()[cnt-1] #2
    data['ub9l'] = sql_pd['S3_R'].rolling(window9).max()[cnt-1] #ub9
    data['lb9l'] = sql_pd['S3_R'].rolling(window9).min()[cnt-1]  #lb9

    # 前回データよりデータ取得 status and price
    tsd = common.select_sql(DB, "select * from " + tablename)
    try:
        data['status'] = int(tsd.iloc[-1]['status'])
        data['price'] = float(tsd.iloc[-1]['price'])
    except:
        data['status'] = 0
        data['price'] = 0
    common.to_number(data)
    #仕切り戦略
    if data['S3_R'] > data['ub9l'] and data['status'] < 0:  # exit short-position
        data['S_PL'] = data['price'] - data['S3_R']
        data['price'] = ""
        data['status'] = 0
        status = -2
    elif data['S3_R'] < data['lb9l'] and data['status'] > 0:  # exit short-position
        data['L_PL'] = data['S3_R']-data['price']
        data['price'] = ""
        data['status'] = 0
        status = 2
    #仕掛け戦略
    elif data['S3_R'] < data['lb0l'] and data['status'] == 0 :  # entry short-position
        data['price'] = data['S3_R']
        data['status'] = -1
        status = -1
    elif data['S3_R'] > data['ub0l'] and data['status'] == 0 :  # entry short-position
        data['price'] = data['S3_R']
        data['status'] = 1
        status = 1
    else:
        status = 0
    #データ書き込み
    common.insertDB3(DB, tablename, data)
    return status


# フィルター付き高値・安値のブレイクアウト
def simple_f(window0, window9, f0, f9, tablename, sql_pd, DB):
    # 過去データよりデータ加工
    data = {}
    cnt = len(sql_pd) - 1
    data['max_s' + str(window0)] = sql_pd['S3_R'].rolling(window0).max()[cnt] #ub0
    data['min_s' + str(window0)] = sql_pd['S3_R'].rolling(window0).min()[cnt] #2
    data['max_e' + str(window9)] = sql_pd['S3_R'].rolling(window9).max()[cnt] #ub9
    data['min_e' + str(window9)] = sql_pd['S3_R'].rolling(window9).min()[cnt] #lb9
    data['avg_l' + str(f0)] = sql_pd['S3_R'].rolling(f0).mean()[cnt] #f0
    data['avg_s' + str(f9)] = sql_pd['S3_R'].rolling(f9).mean()[cnt]  #f9
    data['S3_R'] = float(sql_pd['S3_R'][-1:])

    # 前回データよりデータ取得 status and price
    tsd = common.select_sql(DB, "select *,rowid from " + tablename)
    try:
        data['status'] = int(tsd.iloc[-1]['status'])
        data['price'] = float(tsd.iloc[-1]['price'])
    except:
        data['status'] = 0
        data['price'] = 0
    common.to_number(data)

    #仕切り戦略
    if data['S3_R'] > data['max_e' + str(window9)] and data['status'] < 0:  # exit short-position
        data['S_PL'] = data['price'] -data['S3_R']  # レポート用
        data['price'] = ""
        data['status'] = 0
        status = -2
    elif data['S3_R'] < data['min_e' + str(window9)] and data['status'] > 0:  # exit long-position
        data['L_PL'] = data['S3_R']-data['price']   # レポート用
        data['price'] = ""
        data['status'] = 0
        status = 2
    #仕掛け戦略
    elif data['S3_R'] < data['min_s' + str(window0)] and data['status'] == 0 and data['avg_s' + str(f9)] > data['avg_l' + str(f0)]:
        data['price'] = data['S3_R']
        data['status'] = -1
        status = -1
    elif data['S3_R'] > data['max_s' + str(window0)] and data['status'] == 0 and data['avg_s' + str(f9)] < data['avg_l' + str(f0)]:
        data['price'] = data['S3_R']
        data['status'] = 1
        status = 1
    else:
        status = 0
    #データ書き込み
    common.insertDB3(DB, tablename, data)
    return status

def ma_std(window, multi, tablename, sql_pd, DB):
    # 過去データよりデータ加工
    data = {}

    # 仕掛け処理更新
    cnt = len(sql_pd) - 1
    data['ma_m'] = sql_pd['S3_R'].rolling(window).mean()[cnt]
    data['ma_s'] = sql_pd['S3_R'].rolling(window).std()[cnt]
    data['ub'] = data['ma_m'] + data['ma_s'] * multi
    data['lb'] = data['ma_m'] - data['ma_s'] * multi
    data['S3_R'] = float(sql_pd['S3_R'][cnt])

    # 前回データよりデータ取得 status and price
    tsd = common.select_sql(DB, "select * from " + tablename)
    try:
        data['status'] = int(tsd.iloc[-1]['status'])
        data['price'] = float(tsd.iloc[-1]['price'])
    except:
        data['status'] = 0
        data['price'] = 0
    common.to_number(data)

    #仕切り戦略
    if data['S3_R'] > data['ma_m'] and data['status'] < 0:  # exit short-position
        data['S_PL'] = data['price'] - data['S3_R']
        data['price'] = ""
        data['status'] = 0
        status = -2
    elif data['S3_R'] < data['ma_m'] and data['status'] > 0:  # exit short-position
        data['L_PL'] = data['S3_R']-data['price']
        data['price'] = ""
        data['status'] = 0
        status = 2
    #仕掛け戦略
    elif data['S3_R'] < data['lb']  and data['status'] == 0 :  # entry short-position
        data['price'] = data['S3_R']
        data['status'] = -1
        status = -1
    elif data['S3_R'] > data['ub'] and data['status'] == 0 :  # entry short-position
        data['price'] = data['S3_R']
        data['status'] = 1
        status = 1
    else:
        status = 0
    print(data)
    #データ書き込み
    common.insertDB3(DB, tablename, data)
    return status



if __name__ == '__main__':  # 土曜日は5 datetime.datetime.now().weekday()

    FX_DB = common.save_path('I07_fx.sqlite')
    FX_DB_BYBY = common.save_path('B03_fx_stg.sqlite')

    code = u'GBP/JPY'
    sqls = 'select "%(key1)s" as S3_R from %(table)s where rowid > (select max(rowid) from %(table)s)-1000' % {'table': 'gmofx', 'key1': code}
    sql_pd = common.select_sql(FX_DB,sqls)
    sql_pd['S3_R'] = sql_pd['S3_R'].astype(np.float64)

    window=100
    multi=2.5
    tablename = code.replace("/","") + "_ma_std"
    PL = ma_std(window, multi, tablename, sql_pd, FX_DB_BYBY)

    print("end", __file__)
