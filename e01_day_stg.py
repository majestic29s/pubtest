import pandas as pd
import os
import sys
from selenium import webdriver
from bs4 import BeautifulSoup
import re
import common
sys.path.append(common.LIB_DIR)
import f02_gmo
import f03_ctfx
import datetime

class e01_day_stg(object):
    def __init__(self):
        self.send_msg = ""
        self.INFO_DB = common.save_path('cfd_byby.sqlite')
    # 3つの移動平均を使った戦略


    def breakout_ma_three(self, window0, window9, window5, col,  table):
        status = 0
        data = {'buy': "", 'sell': "", 'm0': "", 'm5': "",'m9': "", 'ShortPL': "", 'LongPL': "", 'Close': "", 'sum': ""}
        sqls = 'select "%(key1)s" from %(table)s where rowid=(select max(rowid) from %(table)s) ;' % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['Close'] = float(sql_pd.loc[0, col])
        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_three"
        common.insertDB3(self.INFO_DB, tablename, data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select *,rowid from %(table)s" % {'table': tablename}
        tsd = common.select_sql(self.INFO_DB, sqls)
        tsd.Close.dropna()
        cnt = len(tsd) - 1
        if cnt < 10:
            return status
        data['m0'] = tsd.Close.rolling(window0).mean().shift(1)[cnt]
        data['m9'] = tsd.Close.rolling(window9).mean().shift(1)[cnt]
        data['m5'] = tsd.Close.rolling(window5).mean().shift(1)[cnt]

        #tsd[["Close"]] = tsd[["Close"]].astype(float)
        # sell_key = 1-para_key
        # レポート用
        # init----------------------------------
        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'sell'] is None or tsd.loc[cnt2, 'sell'] == "":
            sell = 0
        else:
            sell = float(tsd.loc[cnt2, 'sell'])

        if tsd.loc[cnt2, 'buy'] is None or tsd.loc[cnt2, 'buy'] == "":
            buy = 0
        else:
            buy = float(tsd.loc[cnt2, 'buy'])

        try:
            data['sum'] = float(tsd.loc[cnt2, 'sum'])
        except:
            data['sum'] = 0

        data['sell'] = tsd.loc[cnt2, 'sell']
        data['buy'] = tsd.loc[cnt2, 'buy']
        common.to_number(data)
        # entry short-position
        if sell == 0 and data['m0'] < data['m5'] < data['m9']:
            data['sell'] = data['Close']
            status = -1
        if (data['m0'] > data['m5'] or data['m5'] > data['m9']) and sell != 0:  # exit short-position
            data['ShortPL'] = sell-data['Close']
            data['sell'] = 0
            data['sum'] += data['ShortPL']
            status = -2
        # entry short-position
        if buy == 0 and data['m0'] > data['m5'] > data['m9']:
            data['buy'] = data['Close']
            status = 1
        if (data['m0'] < data['m5'] or data['m5'] < data['m9']) and buy != 0:  # exit short-position
            data['LongPL'] = data['Close']-buy
            data['buy'] = 0
            data['sum'] += data['LongPL']
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        sqls = common.create_update_sql(
            self.INFO_DB, data, tablename, sql_pd['rowid'][0])

    # クロスオーバー移動平均

    def breakout_ma_two(self, window0, window9, col,  table):
        status = 0
        data = {'buy': "", 'sell': "", 'm0': "",'m9': "", 'ShortPL': "", 'LongPL': "", 'sum': ""}
        sqls = "select %(key1)s from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['Close'] = float(sql_pd.loc[0, col])

        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_two"
        common.insertDB3(self.INFO_DB, tablename, data)
        sqls = "select *,rowid from " + tablename
        tsd = common.select_sql(self.INFO_DB, sqls)
        tsd.Close.dropna()
        cnt = len(tsd) - 1
        if cnt < 10:
            return status

        data['m0'] = tsd.Close.rolling(window0).mean().shift(1)[cnt]
        data['m9'] = tsd.Close.rolling(window9).mean().shift(1)[cnt]

        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'sell'] is None or tsd.loc[cnt2, 'sell'] == "":
            sell = 0
        else:
            sell = float(tsd.loc[cnt2, 'sell'])

        if tsd.loc[cnt2, 'buy'] is None or tsd.loc[cnt2, 'buy'] == "":
            buy = 0
        else:
            buy = float(tsd.loc[cnt2, 'buy'])

        try:
            data['sum'] = float(tsd.loc[cnt2, 'sum'])
        except:
            data['sum'] = 0

        data['sell'] = tsd.loc[cnt2, 'sell']
        data['buy'] = tsd.loc[cnt2, 'buy']
        common.to_number(data)
        status = 0
        if data['m0'] < data['m9'] and sell == 0:  # entry short-position
            data['sell'] = data['Close']
            status = -1
        if data['m0'] > data['m9'] and sell != 0:  # exit short-position
            data['ShortPL'] = sell-data['Close']  # レポート用
            data['sell'] = 0
            data['sum'] += data['ShortPL']
            status = -2
        if data['m0'] > data['m9'] and buy == 0:  # entry short-position
            data['buy'] = data['Close']
            status = 1
        if data['m0'] < data['m9'] and buy != 0:  # exit short-position
            data['LongPL'] = data['Close']-buy  # レポート用
            data['buy'] = 0
            data['sum'] += data['LongPL']
            status =  2
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        sqls = common.create_update_sql(self.INFO_DB, data, tablename, sql_pd['rowid'][0])
        return status
    # フィルター付き高値・安値のブレイクアウト
#        window0 = 2 window9 = 2 f0 = 12 f9 = 2
    def breakout_simple_f(self,  window0, window9, f0, f9, col,  table):
        status = 0
        data = {'buy': "", 'sell': "", 'sum': "",'ShortPL': "", 'LongPL': ""}
        sqls = "select %(key1)s from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['Close'] = float(sql_pd.loc[0, col])

        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_simple_f"
        common.insertDB3(self.INFO_DB, tablename, data)
        sqls = "select *,rowid from " + tablename
        tsd = common.select_sql(self.INFO_DB, sqls)
        tsd.Close.dropna()
        cnt = len(tsd) - 1
        if cnt < 10:
            return 0
        data['ub0'] = tsd.Close.rolling(window0).max().shift(1)[cnt] #2
        data['lb0'] = tsd.Close.rolling(window0).min().shift(1)[cnt] #2
        data['ub9'] = tsd.Close.rolling(window9).max().shift(1)[cnt] #2
        data['lb9'] = tsd.Close.rolling(window9).min().shift(1)[cnt] #2
        data['f0'] = tsd.Close.rolling(f0).mean().shift(1)[cnt] #12
        data['f9'] = tsd.Close.rolling(f9).mean().shift(1)[cnt] #2
        # init----------------------------------
        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'sell'] is None or tsd.loc[cnt2, 'sell'] == "":
            sell = 0
        else:
            sell = float(tsd.loc[cnt2, 'sell'])

        if tsd.loc[cnt2, 'buy'] is None or tsd.loc[cnt2, 'buy'] == "":
            buy = 0
        else:
            buy = float(tsd.loc[cnt2, 'buy'])

        try:
            data['sum'] = float(tsd.loc[cnt2, 'sum'])
        except:
            data['sum'] = 0

        data['sell'] = tsd.loc[cnt2, 'sell']
        data['buy'] = tsd.loc[cnt2, 'buy']
        common.to_number(data)
        c = data['Close']
        status = 0
        if c < data['ub0'] and sell == 0 and data['f9'] > data['f0']:
            data['sell'] = c
            status = -1
        elif c > data['ub9'] and sell != 0:  # exit short-position
            data['ShortPL'] = sell-c  # レポート用
            data['sell'] = 0
            status =  -2
        elif c > data['ub0'] and buy == 0 and data['f9'] < data['f0']:
            data['buy'] = c
            status = 1
        elif c < data['lb9'] and buy != 0:  # exit short-position
            data['LongPL'] = c-buy  # レポート用
            data['buy'] = 0
            status = 2
        #仕切りチェック
        if status == -1 and buy != 0:
            self.byby_exec_fx(2, col, 1)
        if status == 1 and sell != 0:
            self.byby_exec_fx(-2, col, 1)

        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        sqls = common.create_update_sql(self.INFO_DB, data, tablename, sql_pd['rowid'][0])

        return status

    def stg_main(self):
        window0 = 32
        window9 = 22
        code = u'金スポット'
        table = '_gmo_info'
        PL = self.breakout_ma_two(window0, window9, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 32
        window9 = 12
        code = u'白金スポット'
        table = '_gmo_info'
        PL = self.breakout_ma_two(window0, window9, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 42
        window9 = 2
        window5 = 22
        code = u'白金スポット'
        table = '_gmo_info'
        PL = self.breakout_ma_three(window0, window9, window5, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 2
        window9 = 2
        f0 = 12
        f9 = 2
        code = u'米NQ100'
        table = '_gmo_info'
        PL = self.breakout_simple_f(window0, window9, f0, f9, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 42
        window9 = 52
        window5 = 2
        code = u'米30'
        table = '_gmo_info'
        PL = self.breakout_ma_three(window0, window9, window5, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 32
        window9 = 62
        window5 = 2
        code = u'米S500'
        table = '_gmo_info'
        PL = self.breakout_ma_three(window0, window9, window5, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 22
        window9 = 2
        window5 = 80 #後で100に修正
        code = 'EURJPY'
        table = '_gmo_info'
        PL = self.breakout_ma_three(window0, window9, window5, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 22
        window9 = 12
        window5 = 80  # 後で100に修正
        code = 'EURUSD'
        table = '_gmo_info'
        PL = self.breakout_ma_three(window0, window9, window5, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 2
        window9 = 12
        f0 = 80
        f9 = 40
        code = 'AUDJPY'
        table = '_gmo_info'
        PL = self.breakout_simple_f(window0, window9, f0, f9, code, table)
        self.byby_exec_fx(PL, code, 1)

    def byby_exec_fx(self, PL, code, amount):
        result = 0
        if PL in(1,2,-1,-2):
            if PL == 1:
                bybypara = {'code': code, 'amount': amount, 'buysell': '買', 'kubun': '新規','nari_hiki': '', 'settle': 0, 'comment': code + '_成行買い'}
            if PL == 2:
                # 買い決済
                bybypara = {'code': code, 'amount': amount, 'buysell': '買', 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': code + '_買い決済'}
            if PL == -1:
                bybypara = {'code': code, 'amount': amount, 'buysell': '売', 'kubun': '新規','nari_hiki': '', 'settle': 0, 'comment': code + '_成行売り'}
            if PL == -2:
                # 売り決済
                bybypara = {'code': code, 'amount': amount, 'buysell': '売', 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': code + '_売り決済'}
        else:
            return result
        print(PL)
        print(bybypara)
        try:
            if code.count("JPY") or code.count("USD"):
                result, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
            else:
                for iii in range(3):
                    result, msg = f02_gmo.gmo_cfd_exec(bybypara)
                    if msg.count('正常終了'):
                        break
            self.send_msg += bybypara['comment'] + msg + "\n"
        except:
            self.send_msg += "エラー発生" + bybypara['comment'] + "_" + bybypara['buysell']
            bybypara['status'] = -5
            common.insertDB3(self.INFO_DB, "retry", bybypara)
        return result

    def retry_check(self):
        sqls = 'select *,rowid from retry where status < 0 ;'
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            dict_w = {}
            bybypara = dict(row)
            result, msg = f02_gmo.gmo_cfd_exec(bybypara)
            if str(msg).count('正常終了'):
                dict_w['status'] = 0
            else:
                dict_w['status'] = row['status'] + 1
            sqls = common.create_update_sql(self.INFO_DB, dict_w, 'retry', bybypara['rowid']) #最後の引数を削除すると自動的に最後の行

if __name__ == '__main__':  # 土曜日は5 datetime.datetime.now().weekday()
    info = e01_day_stg()
    argvs = sys.argv
    if argvs[1] == "daily_cfd":
        f02_gmo.info_get()
        info.stg_main()

    t = datetime.datetime.now()
    i = int(t.strftime("%M"))
    if argvs[1] == "retry_check" and i < 10:
        info.retry_check()

    common.mail_send(u'CFDトレード', info.send_msg)

    print("end", __file__)
