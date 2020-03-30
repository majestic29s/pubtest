#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import sleep
import csv, os ,sys
import datetime

#独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)
import f01_sbi
import f02_gmo
import f03_ctfx
import f04_OANDA

FX_DB = common.save_path('I07_fx.sqlite')
FX_DB_BYBY = common.save_path('B03_fx_stg.sqlite')

#passconfig
import configparser
config = configparser.ConfigParser()
config.read([common.PASS_FILE])
USER_ID=config.get('gmo','USER_ID')
PASSWORD=config.get('gmo','PASSWORD')
PASS=config.get('gmo','PASS')

class kabucom:
    def __init__(self,num):
        self.send_msg = ""

    #移動平均を基準とした上限・下限のブレイクアウト
    def breakout_ma_std(self, window, multi, para_key1, para_key2, col, title, type):
        data = {'S3_R':'','ma_m':'','ma_s':'','ub':'','lb':'','status':'0','hold':'','L_PL':'', 'S_PL':''}
        print("code",col)
        #GMOFX情報取得
        stuts = 0
        sqls = "select now,\"" + col + "\" from gmofx where rowid=(select max(rowid) from gmofx);"
        sql_pd = common.select_sql(FX_DB,sqls)
        data['S3_R'] = sql_pd.loc[0,col]

        tablename = "D_" + col.replace(r"/", "") + "_" + title

        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL or status = ''" % {'table':tablename}
        common.sql_exec(FX_DB_BYBY, sqls)
        #データ更新、データフレームに引き出す
        common.insertDB3(FX_DB_BYBY, tablename, data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select now," + col_name + " from " + tablename
        tsd = common.select_sql(FX_DB_BYBY, sqls)
        #仕掛け処理更新
        if len(tsd) > window:
            last_row = len(tsd) - 1
            tsd['ma_m']=tsd.S3_R.rolling(window).mean().dropna()
            tsd['ma_s']=tsd.S3_R.rolling(window).std()
            tsd['ub']=tsd.ma_m+tsd.ma_s*multi
            tsd['lb']=tsd.ma_m-tsd.ma_s*multi
            tsd['status']=tsd['status'].shift(1)
            tsd['hold']=tsd['hold'].shift(1)
            ind = datetime.datetime.now()
            c = round(float(tsd.S3_R[last_row]),4)
            ma_m = round(float(tsd.ma_m[last_row]),4)
            ma_s = round(float(tsd.ma_s[last_row]),4)
            ub = round(float(tsd.ub[last_row]),4)
            lb = round(float(tsd.lb[last_row]),4)
            L_PL = ""
            S_PL = ""
#            print("AAA",type(tsd.status[last_row]))
            print("BBB", tsd.status[last_row])
            status = int(float(tsd.status[last_row]))
            hold = tsd.hold[last_row]

            if hold != "":
                hold = round(float(tsd.hold[last_row]),4)

            if c<lb and status==0 and ind.hour >= para_key1 and ind.hour <= para_key2 :#entry short-position
                stuts = -1 * type
                hold=c
                status=-1
                #売り仕掛け

            if c>ma_m and status<0:#exit short-position
                stuts = -2 * type
                S_PL=round(float(hold-c),4)
                hold=""
                status=0
                #売り仕切り


            if c>ub and status==0 and ind.hour >= para_key1 and ind.hour <= para_key2:#entry short-position
                stuts = 1 * type
                hold=c
                status=1
                #買い仕掛け

            if c<ma_m and  status>0:#exit short-position
                stuts = 2 * type
                L_PL=round(float(c-hold),4)
                hold=""
                status=0
                #買い仕切り

            dict_w = {'ma_m':ma_m,'ma_s':ma_s,'ub':ub,'lb':lb,'status':status,'hold':hold,'L_PL':L_PL,'S_PL':S_PL}
            sqls = common.create_update_sql(FX_DB_BYBY, dict_w, tablename)

            return stuts


    #過去の高値・安値を用いたブレイクアウト戦略
    def breakout_simple(self, window0, window9, para_key1, para_key2, col, title, type):
        data = {'S3_R':'','ub0':'','lb0':'','ub9':'','lb9':'','status':'0','hold':'','L_PL':'', 'S_PL':''}
        #GMOFX情報取得
        stuts = 0
        sqls = "select now,\"" + col + "\" from gmofx where rowid=(select max(rowid) from gmofx) ;"
        sql_pd = common.select_sql(FX_DB,sqls)
        data['S3_R'] = sql_pd.loc[0,col]

        tablename = "D_" + col.replace(r"/", "") + "_" + title
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL or status = ''" % {'table':tablename}
        common.sql_exec(FX_DB_BYBY, sqls)

        #データ更新、データフレームに引き出す
        common.insertDB3(FX_DB_BYBY,tablename,data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select now," + col_name + " from " + tablename
        tsd = common.select_sql(FX_DB_BYBY, sqls)

        #仕掛け処理更新
        if len(tsd) > window9:
            y=tsd
            y['ub0']=y['S3_R'].rolling(window0).max()
            y['lb0']=y['S3_R'].rolling(window0).min()
            y['ub9']=y['S3_R'].rolling(window9).max()
            y['lb9']=y['S3_R'].rolling(window9).min()
            y['status']=y['status'].shift(1)
            y['hold']=y['hold'].shift(1)
            last_row = len(y) - 1
            last_row2 = len(y) - 2
            try:
                c = round(float(y.S3_R[last_row]), 4)
            except:
                print(y.S3_R[last_row])
            ind = datetime.datetime.now()
            ub0 = round(float(y.ub0[last_row]),4)
            lb0 = round(float(y.lb0[last_row]),4)
            ub9 = round(float(y.ub9[last_row]),4)
            lb9 = round(float(y.lb9[last_row]),4)
            #追加
            ub0l = round(float(y.ub0[last_row2]),4)
            lb0l = round(float(y.lb0[last_row2]),4)
            ub9l = round(float(y.ub9[last_row2]),4)
            lb9l = round(float(y.lb9[last_row2]),4)

            L_PL = ""
            S_PL = ""
            status = int(y.status[last_row])
            hold = y.hold[last_row]

            if hold != "":
                hold = round(float(y.hold[last_row]), 4)

            if c<lb0l and status==0 and para_key1 <= ind.hour <= para_key2:#entry short-position
                stuts = -1 * type
                #売り仕掛け(typeを使って買い売り逆転)
                hold=c
                status=-1

            if c>ub9l and status<0:#exit short-position
                stuts = -2 * type
                #売り仕切り
                S_PL=hold-c
                hold=""
                status=0

            if c>ub0l and status==0 and para_key1 <= ind.hour <= para_key2:#entry short-position
                stuts = 1 * type
                #買い仕掛け
                hold=c
                status=1

            if c<lb9l and status>0:#exit short-position
                stuts = 2 * type
                #買い仕切り
                L_PL=c-hold
                hold=""
                status=0

            dict_w = {'ub0':ub0,'lb0':lb0,'ub9':ub9,'lb9':lb9,'status':status,'hold':hold,'L_PL':L_PL,'S_PL':S_PL}
            sqls = common.create_update_sql(FX_DB_BYBY, dict_w, tablename)

            return stuts


    # 3つの移動平均を使った戦略
#    def breakout_simple(self, window0, window9, para_key1, para_key2, col, title, type):
    def breakout_ma_three(self, window0, window5, window9, col):
        stuts = 0
        data = {'L_flag': "", 'S_flag': "", 'S_PL': "", 'L_PL': "", 'S3_R': "", 'status': "0",'S_SUM':"",'L_SUM':""}
        #GMOFX情報取得
        sqls = "select now,\"" + col + "\" from gmofx where rowid=(select max(rowid) from gmofx) ;"
        sql_pd = common.select_sql(FX_DB,sqls)
        data['S3_R'] = sql_pd.loc[0, col]

        tablename = "D_" + col.replace(r"/", "") + "_breakout_ma_three"
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL or status = ''" % {'table':tablename}
        common.sql_exec(FX_DB_BYBY, sqls)
        #データ更新、データフレームに引き出す
        common.insertDB3(FX_DB_BYBY, tablename, data)
        print(tablename,data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select now," + col_name + " from " + tablename
        tsd = common.select_sql(FX_DB_BYBY, sqls)
        tsd.S3_R.dropna()
        cnt = len(tsd) - 1
        if cnt < 250:
            return stuts
        data['avg_'+str(window0)] = tsd.S3_R.rolling(window0).mean().shift(1)[cnt]
        data['avg_'+str(window5)] = tsd.S3_R.rolling(window5).mean().shift(1)[cnt]
        data['avg_' + str(window9)] = tsd.S3_R.rolling(window9).mean().shift(1)[cnt]
        data['status'] =tsd.status.shift(1)[cnt]

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
        if S_flag == 0 and data['avg_'+str(window0)] < data['avg_'+str(window5)] < data['avg_'+str(window9)]:
            data['S_flag'] = data['S3_R']
            stuts = -1
            data['status'] = -1
        elif (data['avg_'+str(window0)] > data['avg_'+str(window5)] or data['avg_'+str(window5)] > data['avg_'+str(window9)]) and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']
            data['S_flag'] = 0
            stuts = -2
            data['status'] = 0
        # entry short-position
        elif L_flag == 0 and data['avg_'+str(window0)] > data['avg_'+str(window5)] > data['avg_'+str(window9)]:
            data['L_flag'] = data['S3_R']
            stuts = 1
            data['status'] = 1
        elif (data['avg_'+str(window0)] < data['avg_'+str(window5)] or data['avg_'+str(window5)] < data['avg_'+str(window9)]) and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag
            data['L_flag'] = 0
            stuts = 2
            data['status'] = 0
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(FX_DB_BYBY, sqls)
        sqls = common.create_update_sql(FX_DB_BYBY, data, tablename, sql_pd['rowid'][0])
        return stuts

    # 4つの移動平均を使った戦略
    def breakout_ma_four(self, window0, window5,window9,window100,col):
        stuts = 0
        data = {'L_flag': "", 'S_flag': "", 'S_PL': "", 'L_PL': "", 'S3_R': "", 'status': "0",'S_SUM':"",'L_SUM':""}
        #GMOFX情報取得S_PL,S_SUM
        sqls = "select now,\"" + col + "\" from gmofx where rowid=(select max(rowid) from gmofx) ;"
        sql_pd = common.select_sql(FX_DB,sqls)
        data['S3_R'] = sql_pd.loc[0, col]

        tablename = "D_" + col.replace(r"/", "") + "_breakout_ma_four"
        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL or status = ''" % {'table':tablename}
        common.sql_exec(FX_DB_BYBY, sqls)
        #データ更新、データフレームに引き出す
        common.insertDB3(FX_DB_BYBY,tablename,data)
        col_name = ', '.join([k for k in data.keys()])
        sqls = "select now," + col_name + " from " + tablename
        tsd = common.select_sql(FX_DB_BYBY, sqls)
        tsd.S3_R.dropna()
        cnt = len(tsd) - 1
        if cnt < 250:
            return stuts
        data['avg_'+str(window0)] = tsd.S3_R.rolling(window0).mean().shift(1)[cnt]
        data['avg_'+str(window5)] = tsd.S3_R.rolling(window5).mean().shift(1)[cnt]
        data['avg_' + str(window9)] = tsd.S3_R.rolling(window9).mean().shift(1)[cnt]
        data['avg_' + str(window100)] = tsd.S3_R.rolling(window100).mean().shift(1)[cnt]
        data['status'] =tsd.status.shift(1)[cnt]

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
        if S_flag == 0 and data['avg_'+str(window0)] < data['avg_'+str(window5)] < data['avg_'+str(window9)] < data['avg_'+str(window100)]:
            data['S_flag'] = data['S3_R']
            stuts = -1
            data['status'] = -1
        elif (data['avg_'+str(window0)] > data['avg_'+str(window5)] or data['avg_'+str(window5)] > data['avg_'+str(window9)] > data['avg_'+str(window100)]) and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']
            data['S_flag'] = 0
            stuts = -2
            data['status'] = 0
        # entry short-position
        elif L_flag == 0 and data['avg_'+str(window0)] > data['avg_'+str(window5)] > data['avg_'+str(window9)] > data['avg_'+str(window100)]:
            data['L_flag'] = data['S3_R']
            stuts = 1
            data['status'] = 1
        elif (data['avg_'+str(window0)] < data['avg_'+str(window5)] or data['avg_'+str(window5)] < data['avg_'+str(window9)] < data['avg_'+str(window100)]) and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag
            data['L_flag'] = 0
            stuts = 2
            data['status'] = 0
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(FX_DB_BYBY, sqls)
        sqls = common.create_update_sql(FX_DB_BYBY, data, tablename, sql_pd['rowid'][0])
        return stuts

    def fx_byby_exec(self,PL,code,amount,title):

        if PL == 0:
            return 0

        if PL == 1:
            bybypara = {'code': code, 'amount': amount, 'buysell': '買', 'kubun': '新規', 'nari_hiki': '0', 'settle': '', 'comment': title, 'now': '0'}
#            exec_time = f01_sbi.sbi_fx_main(bybypara)
            self.send_msg += title + "_" + code + ":新規買い" + "\n"

        if PL == 2:
            bybypara = {'code': code, 'amount': amount, 'buysell': '買', 'kubun': '決済', 'nari_hiki': '0', 'settle': '', 'comment': title, 'now': '0'}
#            exec_time = f01_sbi.sbi_fx_main(bybypara)
            self.send_msg += title + "_" + code + ":決算買い" + "\n"

        if PL == -1:
            bybypara = {'code': code, 'amount': amount, 'buysell': '売', 'kubun': '新規', 'nari_hiki': '0', 'settle': '', 'comment': title, 'now': '0'}
#            exec_time = f01_sbi.sbi_fx_main(bybypara)
            self.send_msg += title + "_" + code + ":新規売り" + "\n"

        if PL == -2:
            bybypara = {'code': code, 'amount': amount, 'buysell': '売', 'kubun': '決済', 'nari_hiki': '0', 'settle': '', 'comment': title, 'now': '0'}
#            exec_time = f01_sbi.sbi_fx_main(bybypara)
            self.send_msg += title + "_" + code + ":決算売り" + "\n"

        if bybypara['kubun'] == '決済':
            bybypara['settle'] = -1
        else:
            bybypara['settle'] = 0
        exec_time, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
        return exec_time

    def fx_byby_oanda(self,PL,code,amount):
        if PL == 0:
            return 0
        if PL == 1: #新規買い
            f04_OANDA.treade_MARKET(code,amount)
        elif PL == 2:#買い決済
            f04_OANDA.treade_MARKET(code,amount*-1)
        elif PL == -1:#新規売り
            f04_OANDA.treade_MARKET(code,amount*-1)
        elif PL == -2:#売り決済
            f04_OANDA.treade_MARKET(code, amount)
        if PL in (2, -2):
            sleep(5)
            dict_w = f04_OANDA.poji_info()
            for k, v in dict_w.items():
                if k.replace("_", "/") == code and v.replace("-", "") == str(amount):
                    self.send_msg += u'OANDA_未決済ポジション_' + code + "_" + str(amount) + "\n"


    def fx_update(self):
        #追加候補
        #AUDJPY_162_252_302_5_breakout_m
        #USDJPY_302_2.5_1_breakout_ma_std.csv

        #BacktestReport
        byby = 'breakout_ma_std'
        window=350
        window=100
        multi=2.5
        code = u'GBP/JPY'
        s_time = 2
        e_time = 15
        PL = self.breakout_ma_std(window,multi,s_time,e_time,code,byby,1)
#        times = self.fx_byby_exec(PL,code,2,byby)

        #breakout_simple
        byby = "breakout_simple"
        window0=20
        window9=10
        code = u'AUD/JPY'
        s_time = 3
        e_time = 6
        PL=self.breakout_simple(window0,window9,s_time,e_time,code,byby,-1)
#        times = self.fx_byby_exec(PL,code,1,byby)

        #breakout_ma_three
        byby = 'breakout_ma_three'
        window0 = 60
        window5 = 150
        window9 = 400
        code = u'GBP/JPY'
        PL = self.breakout_ma_three(window0, window5, window9, code)
        times = self.fx_byby_exec(PL, code, 3, byby)  #30→3
        before = str(f04_OANDA.poji_info())
        try:
            self.fx_byby_oanda(PL, code, 1000)
        except:
            self.send_msg += "OANDA処理エラー_breakout_ma_three:\n前:" + before + "\n後:" + str(f04_OANDA.poji_info())
        window0 = 60
        window5 = 170
        window9 = 550
        code = u'USD/JPY'
        PL = self.breakout_ma_three(window0, window5, window9, code)
        times = self.fx_byby_exec(PL, code, 3, byby)

        window0 = 70
        window5 = 200
        window9 = 250
        code = u'EUR/USD'
        PL = self.breakout_ma_three(window0, window5, window9, code)
#        times = self.fx_byby_exec(PL,code,3,byby)
        #トレーニング中
        window0 = 160
        window5 = 100
        window9 = 2
        code = u'AUD/USD'
        PL = self.breakout_ma_three(window0, window5, window9, code)
#        times = self.fx_byby_exec(PL,code,3,byby)

        #AUDJPY_5_40_850_800_0_breakout_ma_four.csv
        window0 = 5
        window5 = 40
        window9 = 850
        window100 = 800
        code = u'AUD/JPY'
        PL = self.breakout_ma_four(window0, window5, window9,window100, code)
        times = self.fx_byby_exec(PL,code,3,byby)

        #EURUSD_25_10_850_800_0_breakout_ma_four.csv
        window0 = 25
        window5 = 10
        window9 = 850
        window100 = 800
        code = u'EUR/USD'
        PL = self.breakout_ma_four(window0, window5, window9,window100, code)
#        times = self.fx_byby_exec(PL,code,3,byby)



    def ctfx_poji_check(self):
        ok_msg = ""
        INFO_DB = 'B03_fx_stg.sqlite'
        codes, types, amounts = f03_ctfx.f03_ctfx_main({'kubun': 'ポジションチェック', 'amount': 'all'})
        #チェック不要なデータを削除
        for ii in reversed(range(len(codes))):
            if amounts[ii] not in ('3,000','30,000'):
                del codes[ii]
                del types[ii]
                del amounts[ii]

        # 全テーブル情報取得
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(INFO_DB, sqls)
        for i, rrow in sql_pd.iterrows():
            table_name = rrow['name']
            sp_work = table_name.split("_")
            if len(sp_work) != 5 or table_name in ('D_GBPJPY_breakout_ma_std','D_AUDJPY_breakout_simple','D_EURUSD_breakout_ma_three','D_EURUSD_breakout_ma_four','D_AUDUSD_breakout_ma_three','D_AUDJPY_breakout_ma_four'):
                continue
            code = sp_work[1]
            sqls = "select status from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
            sql_pdd = common.select_sql(INFO_DB, sqls)
            if len(sql_pdd) > 0:
                print('sql_pdd',table_name)
                if str(sql_pdd['status'][0]) != '0':
                    if float(sql_pdd['status'][0]) > 0:
                        type_w = "買"
                    elif float(sql_pdd['status'][0]) < 0:
                        type_w = "売"
                    else:
                        type_w = "ステータスが間違っているよ"
                    #FXのポジションチェック
                    for ii in range(len(codes)):
                        if codes[ii] == code and types[ii] == type_w:
                            del codes[ii]
                            del types[ii]
                            del amounts[ii]
                            ok_msg += u'FXポジション一致_' + code + "_" + type_w + "\n"
                            break
                    else:
                        self.send_msg += u'FXポジションなし_' + code + "_" + type_w + "\n"
    #                    if table_name == 'D_GBPJPY_breakout_ma_three':
    #                        amount = 30
    #                    else:
                        amount = 3
                        bybypara = {'code': code, 'amount': amount, 'buysell': type_w, 'kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': code + '_成行'}
                        print(f03_ctfx.f03_ctfx_main(bybypara))


        if len(codes) > 0:
            self.send_msg += u'未決済銘柄あり_' + '_'.join([k for k in codes]) + "\n" + '_'.join([k for k in types]) + "\n" + '_'.join([k for k in amounts]) + "\n" + ok_msg
            for ii in range(len(codes)):
                code = codes[ii][:3] + '/' + codes[ii][3:]
                sp_work = amounts[ii].split(',')
                bybypara = {'code': code, 'amount': sp_work[0], 'buysell': types[ii], 'kubun': '決済', 'nari_hiki': '', 'settle': -1, 'comment': codes[ii] + '_' + types[ii] + '決済'}
                f03_ctfx.f03_ctfx_main(bybypara)

if __name__ == "__main__":
    info = kabucom(0)
    if int(datetime.datetime.now().strftime("%M")) > 40:
        info.ctfx_poji_check()
    info.fx_update()
    common.mail_send(u'FX取引情報', info.send_msg)

    print("end",__file__)
