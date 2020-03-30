#!/usr/bin/env python
# -*- coding: utf-8 -*-
from time import sleep
from bs4 import BeautifulSoup
from selenium import webdriver
import sys
import urllib.request
import datetime
import time
import lxml.html
import os
import requests
import pybitflyer
import poloniex
import pandas as pd

#独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)


#passconfig
import configparser
config = configparser.ConfigParser()
config.read([common.PASS_FILE])

class bit_info(object):

    def __init__(self):
        self.dir = r"00_bitcoin"
        self.send_msg = ""
        t = datetime.datetime.now()
        self.date = t.strftime("%Y%m%d%H%M")
        self.dateh = t.strftime("%H")
        self.C_flag = 0
        self.DB = common.save_path("B07_bitcoin_stg.sqlite")
        self.DB_INFO = common.save_path("I05_bitcoin.sqlite")
        self.api = pybitflyer.API(api_key=config.get('flyer', 'API_Key'), api_secret=config.get('flyer', 'API_Secret'))


    def investing(self, currency):
        if currency == 'jpy':
            url = "https://jp.investing.com/currencies/btc-" + currency + "-historical-data"
        else:
            url = "https://jp.investing.com/crypto/bitcoin/historical-data"
        ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) '\
            'AppleWebKit/537.36 (KHTML, like Gecko) '\
            'Chrome/55.0.2883.95 Safari/537.36 '
        s = requests.session()
        s.headers.update({'User-Agent': ua})
        for  i in range(7):
            try:
                #リアルタイム処理
                req = s.get(url)
                soup = BeautifulSoup(req.text, "html.parser")
                stocktable =  soup.find('table', {'class':'genTbl closedTbl historicalTbl'})
                aaa = stocktable.findAll('td')[0].text.replace("月", "/").replace("日", "").replace("年", "/")
                break
            except:
                time.sleep(10*(i+1))
        dict_w = {}
        dict_w['date'] = stocktable.findAll('td')[0].text.replace("月","/").replace("日","").replace("年","/")
        dict_w['S3_O'] = stocktable.findAll('td')[2].text.replace(",","")
        dict_w['S3_H'] = stocktable.findAll('td')[3].text.replace(",","")
        dict_w['S3_L'] = stocktable.findAll('td')[4].text.replace(",","")
        dict_w['S3_C'] = stocktable.findAll('td')[1].text.replace(",","")
        stocktable2 = soup.find('ul', {'class': 'bold'})
        dict_w['bid'] = stocktable2.findAll('span')[4].text.replace(",","")
        dict_w['ask'] = stocktable2.findAll('span')[5].text.replace(",","")
        dict_w['S3_V'] = stocktable2.findAll('span')[1].text.replace(",","")
        dict_w['S2_L'] = stocktable2.findAll('span')[8].text.replace(",","") #高値
        dict_w['S2_H'] = stocktable2.findAll('span')[9].text.replace(",", "")  #安値
        common.insertDB3(self.DB, "inves_" + currency, dict_w)

        filename = common.save_path(self.dir , "inv_btc" + currency + ".csv")
        common.csv_save(filename,dict_w)
        #日時処理基本はスキップ
        dict_w = {}
        daytable = "inves_" + currency + "_day"
        dict_w['date'] = stocktable.findAll('td')[7].text.replace("月", "/").replace("日", "").replace("年", "/")
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': daytable}
        sql_pd = common.select_sql(self.DB, sqls)
        if dict_w['date'] != str(sql_pd['date'][0]):
            #前日日付再取得
            dict_w['S2_O'] = float(stocktable.findAll('td')[9].text.replace(",",""))
            dict_w['S2_H'] = float(stocktable.findAll('td')[10].text.replace(",",""))
            dict_w['S2_L'] = float(stocktable.findAll('td')[11].text.replace(",",""))
            dict_w['S2_C'] = float(stocktable.findAll('td')[8].text.replace(",",""))
            TR = max(float(dict_w['S2_H']) , float(sql_pd['S2_C'][0])) - min(float(dict_w['S2_L']) , float(sql_pd['S2_C'][0]))
            dict_w['tra'] = round(TR * 0.85,1)

            dict_w['L_ST'] = round(dict_w['S2_C'] + dict_w['tra'],1)
            dict_w['S_ST'] = round(dict_w['S2_C'] - dict_w['tra'],1)
            #前日の仕掛け
            self.stl_dayend(daytable,dict_w['S2_C'])
            #TRA,TR追加する
            common.insertDB3(self.DB, daytable, dict_w)


        else:
            self.stl_check(daytable)

    def stl_check(self, table_day):
        #日時テーブルから
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_day}
        sql_pd = common.select_sql(self.DB, sqls)
        L_ST = sql_pd['L_ST'][0]
        L_flag = sql_pd['L_flag'][0]
        S_ST = sql_pd['S_ST'][0]
        S_flag = sql_pd['S_flag'][0]
        #リアルタイムテーブルから
        table_min = table_day.replace("_day","")
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_min}
        sql_pd_m = common.select_sql(self.DB, sqls)
        real_price = float(sql_pd_m['S3_C'][0])
        dict_w = {}
        if real_price > float(L_ST) and L_flag is None:
            dict_w['L_flag'] = real_price
            sqls = common.create_update_sql(self.DB, dict_w, table_day) #最後の引数を削除すると自動的に最後の行
        if real_price < float(S_ST) and S_flag is None:
            dict_w['S_flag'] = real_price
            sqls = common.create_update_sql(self.DB, dict_w, table_day) #最後の引数を削除すると自動的に最後の行

    def stl_dayend(self, tablename,S2_C):
        dict_w = {}
        sqls = "select *,rowid from %(table)s order by rowid desc ;" % {'table': tablename}
        sql_pd = common.select_sql(self.DB, sqls)
        #前日仕掛けのチェック値
        L_ST = sql_pd['L_ST'][0]
        L_flag =sql_pd['L_flag'][0]
        S_ST = sql_pd['S_ST'][0]
        S_flag = sql_pd['S_flag'][0]
        #買い仕掛け処理
        if L_flag is not None:
            dict_w['L_PL'] = round(S2_C - float(L_flag),1)
        #売り仕掛け処理
        if S_flag is not None:
            dict_w['S_PL'] = round(float(S_flag) - S2_C,1)
        sqls = common.create_update_sql(self.DB, dict_w, tablename) #最後の引数を削除すると自動的に最後の行

    # 移動平均を基準とした上限・下限のブレイクアウト
    def breakout_ma_std(self, window, multi, para_key1, para_key2, col, title, type):
        data = {}
        # GMOFX情報取得
        stuts = 0
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2);"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])

        tablename = col.replace(r"/", "") + "_" + title
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL" % {'table':tablename}
        common.sql_exec(self.DB, sqls)

        common.insertDB3(self.DB, tablename, data)
        # データ更新、データフレームに引き出す
        sqls = "select * from " + tablename
        tsd = common.select_sql(self.DB, sqls)

        # 仕掛け処理更新
        if len(tsd) > window:
            tsd['ma_m'] = tsd.S3_R.rolling(window).mean().dropna()
            tsd['ma_s'] = tsd.S3_R.rolling(window).std()
            tsd['ub'] = tsd.ma_m+tsd.ma_s*multi
            tsd['lb'] = tsd.ma_m-tsd.ma_s*multi
            tsd['status'] = tsd['status'].shift(1)
            tsd['hold'] = tsd['hold'].shift(1)

            ind = datetime.datetime.now()
            c = round(float(tsd.S3_R[len(tsd)-1]), 4)
            ma_m = round(float(tsd.ma_m[len(tsd)-1]), 4)
            ma_s = round(float(tsd.ma_s[len(tsd)-1]), 4)
            ub = round(float(tsd.ub[len(tsd)-1]), 4)
            lb = round(float(tsd.lb[len(tsd)-1]), 4)
            L_PL = ""
            S_PL = ""
            status = float(tsd.status[len(tsd) - 1])
            hold = tsd.hold[len(tsd)-1]
            try:
                hold = float(tsd.hold[len(tsd)-1])
            except:
                pass

            if c < lb and status == 0 and ind.hour >= para_key1 and ind.hour <= para_key2:  # entry short-position
                stuts = -1 * type
                hold = c
                status = -1
                # 売り仕掛け

            if c > ma_m and status < 0:  # exit short-position
                stuts = -2 * type
                S_PL = round(float(hold-c), 4)
                hold = ""
                status = 0
                # 売り仕切り

            if c > ub and status == 0 and ind.hour >= para_key1 and ind.hour <= para_key2:  # entry short-position
                stuts = 1 * type
                hold = c
                status = 1
                # 買い仕掛け

            if c < ma_m and status > 0:  # exit short-position
                stuts = 2 * type
                L_PL = round(float(c-hold), 4)
                hold = ""
                status = 0
                # 買い仕切り
            dict_w = {'ma_m': ma_m, 'ma_s': ma_s, 'ub': ub,'lb': lb, 'status': status, 'hold': hold, 'L_PL': L_PL, 'S_PL': S_PL}
            sqls = common.create_update_sql(self.DB, dict_w, tablename) #最後の引数を削除すると自動的に最後の行
            return stuts

    # 過去の高値・安値を用いたブレイクアウト戦略
    def breakout_simple(self, window0, window9, para_key1, para_key2, col, title, type):
        data = {}
        # GMOFX情報取得
        stuts = 0
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2) ;"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])

        # データ更新、データフレームに引き出す
        tablename = col.replace(r"/", "") + "_" + title
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL" % {'table':tablename}
        common.sql_exec(self.DB, sqls)

        common.insertDB3(self.DB, tablename, data)

        sqls = "select * from " + tablename
        tsd = common.select_sql(self.DB, sqls)
        # 仕掛け処理更新
        if len(tsd) > window9:
#            y = tsd.dropna()
            y = tsd
            y['ub0'] = y['S3_R'].rolling(window0).max()
            y['lb0'] = y['S3_R'].rolling(window0).min()
            y['ub9'] = y['S3_R'].rolling(window9).max()
            y['lb9'] = y['S3_R'].rolling(window9).min()
            y['status'] = y['status'].shift(1)
            y['hold'] = y['hold'].shift(1)
            c = round(float(y.S3_R[len(y)-1]), 4)
            ind = datetime.datetime.now()
            ub0 = round(float(y.ub0[len(y)-1]), 4)
            lb0 = round(float(y.lb0[len(y)-1]), 4)
            ub9 = round(float(y.ub9[len(y)-1]), 4)
            lb9 = round(float(y.lb9[len(y)-1]), 4)
            # 追加
            ub0l = round(float(y.ub0[len(y)-2]), 4)
            lb0l = round(float(y.lb0[len(y)-2]), 4)
            ub9l = round(float(y.ub9[len(y)-2]), 4)
            lb9l = round(float(y.lb9[len(y)-2]), 4)

            L_PL = ""
            S_PL = ""
            if y.status[len(y)-2] == "" or y.status[len(y)-2] is None:
                status = 0
            else:
                status = int(float(y.status[len(y)-1]))
            if y.hold[len(y)-2] == "" or y.status[len(y)-2] is None:
                hold = 0
            else:
                hold = y.hold[len(y)-1]

            try:
                hold = int(float(y.hold[len(y)-1]))
            except:
                pass
            if c < lb0l and status == 0 and para_key1 <= ind.hour <= para_key2:  # entry short-position
                stuts = -1 * type
                # 売り仕掛け(typeを使って買い売り逆転)
                hold = c
                status = -1

            elif c > ub9l and status < 0:  # exit short-position
                stuts = -2 * type
                # 売り仕切り
                S_PL = hold-c
                hold = ""
                status = 0

            elif c > ub0l and status == 0 and para_key1 <= ind.hour <= para_key2:  # entry short-position
                stuts = 1 * type
                # 買い仕掛け
                hold = c
                status = 1

            elif c < lb9l and status > 0:  # exit short-position
                stuts = 2 * type
                # 買い仕切り
                L_PL = c-hold
                hold = ""
                status = 0
            dict_w = {'ub0': ub0, 'lb0': lb0, 'ub9': ub9,'lb9': lb9, 'status': status, 'hold': hold, 'L_PL': L_PL, 'S_PL': S_PL}
            sqls = common.create_update_sql(self.DB, dict_w, tablename) #最後の引数を削除すると自動的に最後の行
        return stuts

    #フィルター付き高値・安値のブレイクアウト
    def breakout_simple_f(self,window0,window9,f0,f9,col):
        #ub0=max - n0 days
        #lb0=min - n0 days
        #ub9=max - n9 days
        #lb9=min - n9 days
        #filter long - f0 days
        #filter short - f9 days


        data = {}
#        data = {'ub0': "", 'lb0': "", 'ub9': "", 'lb9': "",'f0': "", 'S_PL': "", 'L_PL': "",'f9':"",'hold':"",'status':"0"}
        # GMOFX情報取得
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2) ;"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])
        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_simple_f"
        #前回失敗した場合は削除
#        sqls = "delete from %(table)s where status IS NULL" % {'table':tablename}
#        common.sql_exec(self.DB, sqls)

        common.insertDB3(self.DB, tablename, data)

        sqls = "select * from " + tablename
        tsd = common.select_sql(self.DB, sqls)
        tsd.S3_R.dropna()
        cnt = len(tsd) - 1

        if len(tsd) > f9:
            y = tsd
            y = y.set_index('now')
            cnt = len(y) - 1
            print(y.tail())
            data['ub0']=y['S3_R'].rolling(window0).max().shift(1)[cnt]
            data['lb0']=y['S3_R'].rolling(window0).min().shift(1)[cnt]
            data['ub9']=y['S3_R'].rolling(window9).max().shift(1)[cnt]
            data['lb9']=y['S3_R'].rolling(window9).min().shift(1)[cnt]
            data['f0']=y['S3_R'].rolling(f0).mean().shift(1)[cnt]
            data['f9']=y['S3_R'].rolling(f9).mean().shift(1)[cnt]
            data['status'] = y['status'].shift(1)[cnt]
            data['hold'] = y['hold'].shift(1)[cnt]
            try:
                data['status'] = int(data['status'])
            except:
                print("NONE")
                data['status'] = 0

            print("status1",data['status'],type(data['status']))
            if data['S3_R']<data['lb0'] and data['status']==0 and data['f9']>data['f0'] :#entry short-position
                data['hold']=data['S3_R']
                data['status']=-1
            elif data['S3_R']>data['ub9'] and data['status']<0:#exit short-position
                data['S_PL']=data['hold']-data['S3_R']
                data['status'] = 0
            elif data['S3_R']>data['ub0'] and data['status']==0 and data['f9']<data['f0'] :#entry short-position
                data['hold']=data['S3_R']
                data['status']=1
            elif data['S3_R']<data['ub9'] and data['status']>0:#exit short-position
                data['L_PL']=data['S3_R']-data['hold']
                data['status'] = 0
            print(data)
            sqls = common.create_update_sql(self.DB, data, tablename) #最後の引数を削除すると自動的に最後の行

            return

    # 3つの移動平均を使った戦略
    def breakout_ma_three(self, window0, window5, window9, col):
        stuts = 0
        data = {'L_flag': "", 'S_flag': "", 'm0': "", 'm5': "",'m9': "", 'S_PL': "", 'L_PL': "",'status':"0"}
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2) ;"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])
        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_three"
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL or status = ''" % {'table':tablename}
        common.sql_exec(self.DB, sqls)

        common.insertDB3(self.DB, tablename, data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select *,rowid from " + tablename + ";"
        tsd = common.select_sql(self.DB, sqls)
        tsd.S3_R.dropna()
        cnt = len(tsd) - 1
        data['m0'] = tsd.S3_R.rolling(window0).mean().shift(1)[cnt]
        data['m5'] = tsd.S3_R.rolling(window5).mean().shift(1)[cnt]
        data['m9'] = tsd.S3_R.rolling(window9).mean().shift(1)[cnt]
        data['status'] = tsd['status'].shift(1)[cnt]
        if cnt < window9:
            return stuts
        # sell_key = 1-para_key
        # レポート用
        # init----------------------------------
        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'S_flag'] is None or tsd.loc[cnt2, 'S_flag'] == "":
            S_flag = 0
        else:
            S_flag = float(tsd.loc[cnt2, 'S_flag'])

        if tsd.loc[cnt2, 'L_flag'] is None or tsd.loc[cnt2, 'L_flag'] == "":
            L_flag = 0
        else:
            L_flag = float(tsd.loc[cnt2, 'L_flag'])

        data['S_flag'] = tsd.loc[cnt2, 'S_flag']
        data['L_flag'] = tsd.loc[cnt2, 'L_flag']

        common.to_number(data)
        # entry short-position
        if S_flag == 0 and data['S3_R'] < data['m0'] < data['m5'] < data['m9']:
            #            if S_flag==0 and m0<m5 and m0<m9 and m5<m9 and y[para_val].iloc[i] >= sell_key:#entry short-position
            data['S_flag'] = data['S3_R']
            data['status'] = -1
            stuts = -1
        elif (data['m0'] > data['m5'] or data['m5'] > data['m9'] ) and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']
            data['S_flag'] = 0
            data['status'] = 0
            stuts = -2
        # entry short-position
        elif L_flag == 0 and data['S3_R'] > data['m0'] > data['m5'] > data['m9']:
            #            if L_flag==0 and m0>m5 and m0>m9 and m5>m9 and y[para_val].iloc[i] <= buy_key:#entry short-position
            data['L_flag'] = data['S3_R']
            data['status'] = 1
            stuts = 1
        elif (data['m0'] < data['m5'] or data['m9'] < data['m5']) and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag
            data['L_flag'] = 0
            data['status'] = 0
            stuts = 2
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.DB, sqls)
        sqls = common.create_update_sql(self.DB, data, tablename, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行
        return stuts

    # 4つの移動平均を使った戦略
    def breakout_ma_four(self, window0, window5, window9,window10, col):
        stuts = 0
        data = {'L_flag': "", 'S_flag': "", 'm0': "", 'm5': "",'m9': "",'m10': "", 'S_PL': "", 'L_PL': "",'status':"0"}
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2) ;"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])
        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_four"
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL or status = ''" % {'table':tablename}
        common.sql_exec(self.DB, sqls)

        common.insertDB3(self.DB, tablename, data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select *,rowid from " + tablename + ";"
        tsd = common.select_sql(self.DB, sqls)
        tsd.S3_R.dropna()
        cnt = len(tsd) - 1
        data['m0'] = tsd.S3_R.rolling(window0).mean().shift(1)[cnt]
        data['m5'] = tsd.S3_R.rolling(window5).mean().shift(1)[cnt]
        data['m9'] = tsd.S3_R.rolling(window9).mean().shift(1)[cnt]
        data['m10'] = tsd.S3_R.rolling(window10).mean().shift(1)[cnt]
        data['status'] = tsd['status'].shift(1)[cnt]
        if cnt < window10:
            return stuts
        # sell_key = 1-para_key
        # レポート用
        # init----------------------------------
        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'S_flag'] is None or tsd.loc[cnt2, 'S_flag'] == "":
            S_flag = 0
        else:
            S_flag = float(tsd.loc[cnt2, 'S_flag'])

        if tsd.loc[cnt2, 'L_flag'] is None or tsd.loc[cnt2, 'L_flag'] == "":
            L_flag = 0
        else:
            L_flag = float(tsd.loc[cnt2, 'L_flag'])

        data['S_flag'] = tsd.loc[cnt2, 'S_flag']
        data['L_flag'] = tsd.loc[cnt2, 'L_flag']

        common.to_number(data)
        # entry short-position
        if S_flag == 0 and data['S3_R'] < data['m0'] < data['m5'] < data['m9'] < data['m10']:
            #            if S_flag==0 and m0<m5 and m0<m9 and m5<m9 and y[para_val].iloc[i] >= sell_key:#entry short-position
            data['S_flag'] = data['S3_R']
            data['status'] = -1
            stuts = -1
        elif (data['m5'] > data['m9'] or data['m0'] > data['m5']) and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']
            data['S_flag'] = 0
            data['status'] = 0
            stuts = -2
        # entry short-position
        elif L_flag == 0 and data['S3_R'] > data['m0'] > data['m5'] > data['m9'] > data['m10']:
            #            if L_flag==0 and m0>m5 and m0>m9 and m5>m9 and y[para_val].iloc[i] <= buy_key:#entry short-position
            data['L_flag'] = data['S3_R']
            data['status'] = 1
            stuts = 1
        elif (data['m5'] < data['m9'] or data['m0'] < data['m5']) and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag
            data['L_flag'] = 0
            data['status'] = 0
            stuts = 2
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.DB, sqls)
        sqls = common.create_update_sql(self.DB, data, tablename, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行
        return stuts

    # クロスオーバー移動平均

    def breakout_ma_two(self, window0, window9, col):
        stuts = 0
        data = {'L_flag': "", 'S_flag': "", 'm0': "",'m9': "", 'S_PL': "", 'L_PL': ""}
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2) ;"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])

        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_two"
        common.insertDB3(self.DB, tablename, data)
        sqls = "select *,rowid from " + tablename
        tsd = common.select_sql(self.DB, sqls)
        tsd.S3_R.dropna()
        cnt = len(tsd) -1
        if cnt < 355:
            return stuts

        data['m0'] = tsd.S3_R.rolling(window0).mean().shift(1)[cnt]
        data['m9'] = tsd.S3_R.rolling(window9).mean().shift(1)[cnt]
        data['status'] = tsd['status'].shift(1)[cnt]

        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'S_flag'] is None or tsd.loc[cnt2, 'S_flag'] == "":
            S_flag = 0
        else:
            S_flag = float(tsd.loc[cnt2, 'S_flag'])

        if tsd.loc[cnt2, 'L_flag'] is None or tsd.loc[cnt2, 'L_flag'] == "":
            L_flag = 0
        else:
            L_flag = float(tsd.loc[cnt2, 'L_flag'])

        data['S_flag'] = tsd.loc[cnt2, 'S_flag']
        data['L_flag'] = tsd.loc[cnt2, 'L_flag']

        common.to_number(data)
        if data['m0'] < data['m9'] and S_flag == 0 and L_flag == 0:  # entry short-position
            #            if m0<m9 and S_flag==0 and y[para_val].iloc[i] >= sell_key:#entry short-position
            data['S_flag'] = data['S3_R']
            data['status'] = -1
        elif data['m0'] > data['m9'] and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']  # レポート用
            data['S_flag'] = 0
            data['status'] = 0
        elif data['m0'] > data['m9'] and L_flag == 0  and S_flag == 0:  # entry short-position
            #            if m0>m9 and L_flag==0 and y[para_val].iloc[i] <= buy_key:#entry short-position
            data['L_flag'] = data['S3_R']
            data['status'] = 1
        elif data['m0'] < data['m9'] and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag  # レポート用
            data['L_flag'] = 0
            data['status'] = 0
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.DB, sqls)
        sqls = common.create_update_sql(self.DB, data, tablename, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行
        return stuts

    def fx_byby_exec(self, PL, code, amount):
        exec_time = 0
        if self.C_flag == 0:
            self.check_poji()
            self.C_flag = 1
        if PL == 0:
            return 0
        if PL == 1:
            type_w = "BUY"
            msg_w = ":新規買い_注文番号:"
        if PL == 2:
            type_w = "SELL"
            msg_w = ":決算買い_注文番号:"
        if PL == -1:
            type_w = "SELL"
            msg_w = ":新規売り_注文番号:"
        if PL == -2:
            type_w = "BUY"
            msg_w = ":決算売り_注文番号:"
        exec_time = self.byby_exec(type_w, amount)
        return exec_time

    def byby_exec(self, side, amount,not_db_update = 1):
        # スキップ条件
        path1 = common.STOP_FLAG  # ALLフラグ python_tool\03_lib\stop
        # 個別フラグ python_tool\03_lib\stop03_flyer_jpy.py
        path2 = path1 + os.path.basename(__file__)
        if os.path.exists(path1) or os.path.exists(path2):
            return 0
        # 実ポジションアップデート
        if not_db_update == 1:
            self.poji_update(side, amount)

        # 注文キャンセル
        self.cancelAllOrder()

        # 注文実行
        for i in range(1, 10):
            #サーバのステータスチェック
            health = self.api.gethealth(product_code="FX_BTC_JPY")
            if ((health['status'] == 'STOP') or (health['status'] == 'VERY BUSY')):
                sleep(10)
                continue

            # 板情報取得
            ob = self.orderbook()
            spread = int(ob["asks"][0][0] - ob["bids"][0][0])
            if side == "BUY":
                price = int(ob["asks"][0][0] + spread)
            else:
                price = int(ob["bids"][0][0] - spread)
            print("spread", ob["asks"][0][0], ob["bids"][0][0])
            print("price",price,side,amount)
            b = self.api.sendchildorder(product_code="FX_BTC_JPY", child_order_type="LIMIT", minute_to_expire=6000, side=side, size=amount, price=price)
            try:
                break
            except:
                sleep(10)
        else:
            common.mail_send(u'ビットコイン売買失敗', str(b))
        return b
    # 注文板情報を得る
    def orderbook(self):
        r = requests.get('https://api.bitflyer.jp/v1/board?product_code=FX_BTC_JPY')
        j = r.json()
        return {"bids": [(i["price"], i["size"]) for i in j["bids"]], "asks": [(i["price"], i["size"]) for i in j["asks"]]}
    # 注文が有効かを返す

    # ポジションの有無を確認
    # 建玉情報の取得メソッド
    # side : ポジション方向
    # size : 全建玉のサイズ
    # sfd_valu : sfdが付与されているポジションの合計額

    def getMypos(self):
        side = ""
        size = 0
        sfd_valu = 0
        for i in range(3):
            try:
                pos_dict = self.api.getpositions(product_code='FX_BTC_JPY')
                if 'Message' not in pos_dict:
                    break
            except:
                sleep(30)
        else:
            pos_dict = self.api.getpositions(product_code='FX_BTC_JPY')  #APIからポジション取得できな場合エラー
        # もしポジションがあれば合計値を取得
        if len(pos_dict) > 0 and 'Message' not in pos_dict:
            for pos in pos_dict:
                side = pos["side"]
                size += pos["size"]
                if pos["sfd"] > 0:
                    sfd_valu += pos["size"] * pos["price"]
        return side, size, sfd_valu
    # 注文をキャンセルするメソッド

    def cancelAllOrder(self):
        for i in range(1,6):
            #サーバのステータスチェック
            health = self.api.gethealth(product_code="FX_BTC_JPY")
            if ((health['status'] == 'STOP') or (health['status'] == 'VERY BUSY')):
                sleep(10 * i)
            else:
                break

        self.api.cancelallchildorders(product_code='FX_BTC_JPY')

    def poji_update(self, side, size):
        if side == "SELL":
            size = size * -1
        dict_w = {}
        dict_w['status_cnt'] = size
        table_name = 'poji_table'
        # 現在のポジションチェック
        dict_w['status_cnt'] += self.poji_status()[0]
        dict_w['status_cnt'] = round(dict_w['status_cnt'], 1)
        common.insertDB3(self.DB, table_name, dict_w)

    def poji_status(self):
        # 現在のポジションチェック
        table_name = 'poji_table'
        sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % { 'table': table_name}
        sql_pd = common.select_sql(self.DB, sqls)
        return float(sql_pd.loc[0, 'status_cnt']), sql_pd.loc[0, 'now']

    def check_poji(self):
        # スキップ条件
        path1 = common.STOP_FLAG  # ALLフラグ python_tool\03_lib\stop
        # 個別フラグ python_tool\03_lib\stop03_flyer_jpy.py
        path2 = path1 + os.path.basename(__file__)
        if os.path.exists(path1) or os.path.exists(path2):
            return 0
        side, size, sfd_valu = self.getMypos()
        if side == "SELL":
            size = size * -1
        DBpoji, now = self.poji_status()
        diff_poji = round(DBpoji - size, 2)
        t = datetime.datetime.now()
        m = int(t.strftime("%M"))
#        if diff_poji != 0 and m > 40:
        if diff_poji != 0 :
            if diff_poji > 0:
                result = self.byby_exec("BUY", diff_poji,0)
            if diff_poji < 0:
                result = self.byby_exec("SELL", diff_poji * -1 ,0)
            common.mail_send(u'ビットコイン売買差分発生', "DBポジション:" + str(round(DBpoji,2)) + "\n" + "リアルポジション:" + str(round(size,2)) + "side:" + "\n" + str(result) + "\n")


    def amount_status(self,table_name):
        # 現在のポジションチェック
        sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % { 'table': table_name}
        sql_pd = common.select_sql(self.DB, sqls)
        if len(sql_pd) == 0:
            return 0.0
        else:
            return float(sql_pd['pl'][0]) * 0.1

    def poji_list_update(self):
        #現在のポジションを取得
        sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % { 'table': 'poji_table'}
        sql_pd = common.select_sql(self.DB, sqls)
        status_cnt = float(sql_pd['status_cnt'][0])
#        tables = {'fx_ltp_breakout_ma_std':0.3 , 'fx_ltp_breakout_ma_three':0.1, 'fx_ltp_breakout_ma_two':0.1, 'fx_ltp_breakout_simple':0.1}
        tables = {'fx_ltp_breakout_ma_std':1.5 , 'fx_ltp_breakout_ma_two':0.5}
        dict_w = {}
        for table_name, amount in tables.items():
            col_name = table_name[7:]
            sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % { 'table': table_name}
            sql_pd = common.select_sql(self.DB, sqls)
            print(sqls)
            print(sql_pd)
            dict_w[col_name] = sql_pd['status'][0] * amount #縦玉数
        if float(sum(dict_w.values())) != status_cnt:
            dict_w['status_cnt'] = float(sum(dict_w.values()))

        col = u'fx_ltp'
        sqls = "select now,\"" + col + "\" from btcfx2 where rowid=(select max(rowid) from btcfx2) ;"
        sql_pd = common.select_sql(self.DB_INFO, sqls)
        dict_w['price'] = float(sql_pd.loc[0, col])
        common.create_update_sql(self.DB, dict_w, 'poji_table')

    def bitcoin_stg(self):
        byby = 'breakout_ma_std'
        window = 300
        multi = 2.5
        fx = u'fx_ltp'
        s_time = 0
        e_time = 24
        PL = self.breakout_ma_std(window, multi, s_time, e_time, fx, byby, 1)
        print('Start_breakout_ma_std',PL)
#        times = self.fx_byby_exec(PL, fx, 0.5)
        times = self.fx_byby_exec(PL, fx, 1.5)

        window0 = 100
        window9 = 350
        fx = u'fx_ltp'
        PL = self.breakout_ma_two(window0, window9, fx)
        print('Start_breakout_ma_two',PL)
#        times = self.fx_byby_exec(PL,fx,0.1)
        times = self.fx_byby_exec(PL,fx,0.5)


        # breakout_ma_three 2020/03/23にパラメータ修正
        window0 = 150
        window5 = 350
        window9 = 1500
        fx = u'fx_ltp'
        PL = self.breakout_ma_three(window0, window5, window9, fx)
        print('Strat_breakout_ma_three',PL)
#        times = self.fx_byby_exec(PL, fx, 0.1) #20200205 ポジションが無くなり次第削除
        # ●●●●

        # breakout_simple ×× リアル売買停止中
        byby = "breakout_simple"
        window0 = 20
        window9 = 10
        fx = u'fx_ltp'
        s_time = 0
        e_time = 24
        PL = self.breakout_simple(window0, window9, s_time, e_time, fx, byby, 1)
#        times = self.fx_byby_exec(PL,fx,0.1)


        window0 = 50
        window5 = 250
        window9 = 550
        window10 = 2000
        fx = u'fx_ltp'
        PL = self.breakout_ma_four(window0, window5, window9,window10, fx)
#        times = self.fx_byby_exec(PL, fx, 0.1)

        window0 = 150
        window9 = 2
        f0 = 80
        f9 = 350
        fx = u'fx_ltp'
        PL = self.breakout_simple_f( window0, window9, f0, f9, fx)
#        times = self.fx_byby_exec(PL, fx, 0.1)

    def poloniex_info2(self):
        polo = poloniex.Poloniex()
        for code in ['BTC_ETH', 'USDT_BTC', 'USDC_BTC']:
            try:
                # 5分間隔（サンプリング間隔300秒）で1日分読み込む
                chart_data = polo.returnChartData(code, period=900, start=time.time() - polo.DAY * 1, end=time.time())
            except:
                print("NG")
                return

            #最新DBの値取得
            sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'pol_' + code}
            sql_pd = common.select_sql(self.DB_INFO, sqls)
            db_date = sql_pd['date'][0]
            #DBと比較し新しい場合は追加
            for i in range(len(chart_data) - 1):
                chart_data[i]['date'] = str(datetime.datetime.fromtimestamp(chart_data[i]['date']))
                if chart_data[i]['date'] > db_date:
                    print('pol_' + code,chart_data[i]['date'] , db_date)
                    common.insertDB3(self.DB_INFO, 'pol_' + code, chart_data[i])

    def coinmarketcap_info(self):
        t = datetime.datetime.now()
        H = int(t.strftime("%H"))
        if H == 9:
            table_name = 'D_coinmarket_bitcoin'
            bitcoin = pd.read_html("https://coinmarketcap.com/currencies/bitcoin/historical-data/?start=20190101&end=" + time.strftime("%Y%m%d"))[2]
            bitcoin = bitcoin.assign(Date=pd.to_datetime(bitcoin['Date']))
            bitcoin['Volume'] = bitcoin['Volume'].astype('int64')
            bitcoin = bitcoin.rename(columns={'Open*': 'Open'})
            bitcoin = bitcoin.rename(columns={'Close**': 'Close'})
            dict_w = dict(bitcoin.loc[0])
            sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
            sql_pd = common.select_sql(self.DB_INFO, sqls)
            if sql_pd['Close'][0] != dict_w['Close']:
                common.insertDB3(self.DB_INFO, table_name, dict_w)

if __name__ == "__main__":
    info = bit_info()
    common.log_write("ビットコイン処理_開始終了", __file__)

    window0 = 150
    window9 = 2
    f0 = 80
    f9 = 350
    fx = u'fx_ltp'
#    PL = info.breakout_simple_f( window0, window9, f0, f9, fx)

    #戦略実行
    info.bitcoin_stg()
    info.poji_list_update() #各テーブルのポジションを更新する
    info.investing("jpy")
    info.poloniex_info2()
    info.coinmarketcap_info()
#    info.investing("usd")
    common.mail_send(u'ビットコイン取引情報', info.send_msg)
    common.log_write("ビットコイン処理_正常終了", __file__)

