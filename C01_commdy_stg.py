import pandas as pd
import os
import datetime
import time
from pandas.io import sql
import sys
import glob
from statistics import mean, median, variance, stdev
import numpy as np
# 独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)
import c01_hokushin

BYBY_DB = common.save_path('comm_byby.sqlite')
DB_INFO = common.save_path('comm.sqlite')


class C01_commdy_stg(object):
    def __init__(self):
        self.today = datetime.date.today()
        self.row_arry = []
        self.send_msg = ""
        self.gold_amount = 2
        try:
            self.argvs = argvs[1]
        except:
            self.argvs = 0
            pass

    def round_up_down(self, val, opt, buysell):  # 10 -1
        val = int(val)
        ttt = round(val, opt)
        if val > ttt and buysell == '買':
            return ttt + 10
        if val < ttt and buysell == '売':
            return ttt - 10
        return ttt

    def unit(self, code):
        if code == '東京ゴム':
            return 1
        if code == '東京コーン' or code == '東京ガソリン':
            return -1
        return 0

    def round2(self, code, buy, sell):
        if code == "東京ゴム":
            buy = float(buy)
            sell = float(sell)
        else:
            buy = int(buy)
            sell = int(sell)
        buy = round(buy, self.unit(code))
        sell = round(sell, self.unit(code))
        return buy, sell

    def com_exec(self, table_name, code, val, num, info_table, amount):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        # データ取得
        dict_e, dict_d, dict_n = self.get_data(7, table_name, info_table)
        if self.argvs in ('day17', 'day22', 'day23', 'day24'):#'day23'は削除する予定
            dict_w['S2_O'] = dict_n['始値'] #S2_O
            dict_w['S2_H'] = dict_n['高値'] #S2_H
            dict_w['S2_L'] = dict_n['安値'] #S2_L
            dict_w['S2_C'] = dict_n['終値'] #S2_C
            dict_w['S3_O'] = dict_d['始値'] #S3_O
            print("day22")
        else:
            dict_w['S2_O'] = dict_d['始値'] #S2_O
            dict_w['S2_H'] = dict_d['高値'] #S2_H
            dict_w['S2_L'] = dict_d['安値'] #S2_L
            dict_w['S2_C'] = dict_d['終値'] #S2_C
            dict_w['S3_O'] = dict_e['始値'] #S3_O
        # 前日仕掛けデータ取得
        table_name = table_name + "_" + str(num)
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        print(sql_pd)
        if len(sql_pd) > 0:
            buy, sell = self.round2(code, sql_pd['S2_B_STL'][0], sql_pd['S2_S_STL'][0])
            if dict_w['S2_H'] >= buy: #S2_H
                dict_w['LongPL'] = dict_w['S3_O'] - buy  #LongPL S3_O
                dict_w['LongSum'] = "" #LongSum
                dict_w['LongPL'] = round(dict_w['LongPL'], self.unit(code)) #LongPL
                print("仕掛買い")
            if dict_w['S2_L'] <= sell: #S2_L
                dict_w['ShortPL'] = sell - dict_w['S3_O'] #ShortPL S3_O
                dict_w['ShortSum'] = ""#ShortSum
                dict_w['ShortPL'] = round(dict_w['ShortPL'], self.unit(code))#ShortPL
                print("仕掛売り")
            if sql_pd['memo'][0] == 'shell_nari':
                dict_w['ShortPLNari'] = float(sql_pd['S0_C'][0]) - dict_n['始値'] #ShortPLNari S0_C
                dict_w['ShortSumNari'] = "" #ShortSumNari
                dict_w['ShortPLNari'] = round(dict_w['ShortPLNari'], self.unit(code))#ShortPLNari

            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行

        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        dict_w['S0_O'] = dict_e['始値']#S0_O
        dict_w['S0_C'] = dict_e['終値']#S0_C
        dict_w['memo'] = ""

        # VIX JPYなど
        dict_w.update(common.info_index())
        dict_w.update(common.bloomberg_real())
        # 仕掛値の切り上げ、切り下げ
        if dict_e['高値'] == dict_e['終値'] or dict_e['安値'] == dict_e['終値']:
            bf = val
        else:
            bf = 0
        dict_w['S2_B_STL'], dict_w['S2_S_STL'] = self.round2(code, dict_e['高値'] + bf, dict_e['安値'] - bf)
        #S1_H S1_L
        if num == 9:
            # 翌日データ準備
            dict_w['S00_C'] = dict_d['終値'] #S00_C
            # ATR計算
            print(dict_d['高値'])
            MAX_W = max(float(dict_d['高値']), sql_pd['S00_C'][0])#S00_C
            MIN_W = min(float(dict_d['安値']), sql_pd['S00_C'][0])#S00_C
            dict_w['変動幅'] = round(float(MAX_W - MIN_W), 2)
            dict_w['幅85'] = round(float(dict_w['変動幅'] * 0.85), 2)
            dict_d['終値'] = float(dict_d['終値'])
            # 仕掛値の切り上げ、切り下げ
            dict_w['S2_B_STL'], dict_w['S2_S_STL'] = self.round2(code, dict_w['幅85']+dict_d['終値'], dict_d['終値']-dict_w['幅85'])
            #S1_H S1_L

        if num == 17:
            if code == "東京ゴム":
                # 原油価格取得
                DB_RASHIO = common.save_path('all_info.sqlite')
                sqls = "select 価格,rowid from %(table)s where 名称 = '%(key1)s'" % {'table': 'bloomberg_list', 'key1': 'CL1:COM WTI原油 (NYMEX)'}
                sql_pd2 = common.select_sql(DB_RASHIO, sqls)
                numt = len(sql_pd2)-1
                dict_w['Vora_WTI'] = round(float(sql_pd2.loc[numt, '価格']) / float(sql_pd2.loc[numt - 2, '価格']) - 1, 5)
                #Vora_WTI
            if code == "東京金":
                # 米国金価格取得
                DB_RASHIO = common.save_path('all_info.sqlite')
                sqls = "select 価格,rowid from %(table)s where 名称 = '%(key1)s'" % {'table': 'bloomberg_list', 'key1': 'GC1:COM 金 (CMX)'}
                sql_pd2 = common.select_sql(DB_RASHIO, sqls)
                numt = len(sql_pd2)-1
                dict_w['Vora_Gold'] = round(float(sql_pd2.loc[numt, '価格']) / float(sql_pd2.loc[numt-2, '価格'])-1, 5)
                #Vora_Gold

        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ実行とデータ更新--------- #
        # ////////////////////////////////////////////////// #
        flag = 0  # ０は取引しない
        nari = 0

        if len(sql_pd) > 0:
            if num == 10 or num == 11:
                if code == "東京白金" and self.today.weekday() == 2:
                    flag = 2

                if code == "東京ゴム":
                    flag = -1
                    sqls = "select *,rowid from %(table)s" % {'table': 'GOMU'}
                    sql_pd3 = common.select_sql(DB_INFO, sqls)
                    num = len(sql_pd3)-5
                    dict_w['S1_R'] = float(sql_pd3["現値"][num]) #S1_R
                    if dict_n['終値'] < dict_w['S1_R']:#S1_R
                        print("nari", dict_n['now'], dict_n['終値'],sql_pd3["now"][num], sql_pd3["現値"][num])
                        nari = 1
                        flag_file = common.save_path(common.DROP_DIR, "COMM_" + code + "_売")
                        common.create_file(flag_file)
                        msg = c01_hokushin.hoku_settles('東京ゴム',"買")
                    elif dict_e['高値'] > dict_d['高値']:
                        flag = 1
                if code == "東京コーン":
                    if dict_e['始値'] < dict_e['終値']:
                        flag = 1
                    if dict_e['始値'] > dict_e['終値']:
                        flag = -1
            if num == 9:
                if dict_w['S2_S_STL'] < dict_e['終値'] < dict_w['S2_B_STL'] and self.today.weekday() != 3:
                    #S1_L S1_H
                    flag = 2
            if num == 17:

                if code == "東京ゴム":
                    print(code, dict_e['高値'], dict_n['高値'])
                    if dict_e['安値'] < dict_n['安値']:
                        flag = -1
                    if dict_e['高値'] > dict_n['高値']:
                        if flag == -1:
                            flag = 2
                        else:
                            flag = 1

                elif code == "東京金" and self.today.weekday() == 4:
                    flag = 1

        if flag == -1 or flag == 2:  # flag０は取引しない
            dict_w['amount_s'] = amount #amount_s
            dict_w['memo'] = "shell"
            bybypara = {'code': code, 'amount': dict_w['amount_s'], 'buysell': '売','kubun': '逆指値･成行', 'nari_hiki': '', 'settle': dict_w['S2_S_STL'], 'comment': '逆指値売り'}

            if nari == 1:
                bybypara = {'code': code, 'amount': dict_w['amount_s'], 'buysell': '売','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き売り'}
                dict_w['memo'] = "shell_nari"

            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += code + "_" + msg + ":逆差値_" + str(dict_w['S2_S_STL']) + ":現値_" + str(dict_e['終値']) + "_now_" + str(dict_e['now']) + "\n"
        if flag == 1 or flag == 2:  # flag０は取引しない
            dict_w['amount_b'] = amount
            dict_w['memo'] = "buy"
            bybypara = {'code': code, 'amount': dict_w['amount_b'], 'buysell': '買','kubun': '逆指値･成行', 'nari_hiki': '', 'settle': dict_w['S2_B_STL'], 'comment': '逆指値買い'}
            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += code + "_" + msg + ":逆差値_" + str(dict_w['S2_B_STL']) + ":現値_" + str(dict_e['終値']) + "_now_" + str(dict_e['now']) + "\n"

        common.insertDB3(BYBY_DB, table_name, dict_w)


    def get_data(self, priod, table_name, info_table):
        dict_n = {}
        dict_d = {}
        dict_e = {}

        # 商品前日終値
        DB_RASHIO = common.save_path('all_info.sqlite')
        sqls = "select * from %(table)s where rowid=(select max(rowid) from %(table)s)" % {'table': info_table}
        sql_pd = common.select_sql(DB_RASHIO, sqls)

        dict_d['now'] = sql_pd['now'][0]
        dict_d['始値'] = sql_pd['始値'][0]
        dict_d['高値'] = sql_pd['高値'][0]
        dict_d['安値'] = sql_pd['安値'][0]
        dict_d['終値'] = sql_pd['終値'][0]
        try:
            float(sql_pd['始値L'][0])
            dict_n['now'] = sql_pd['now'][0]
            dict_n['始値'] = sql_pd['始値L'][0]
            dict_n['高値'] = sql_pd['高値L'][0]
            dict_n['安値'] = sql_pd['安値L'][0]
            dict_n['終値'] = sql_pd['終値L'][0]
        except:
            dict_n['now'] = sql_pd['now'][0]
            dict_n['始値'] = sql_pd['始値L1'][0]
            dict_n['高値'] = sql_pd['高値L1'][0]
            dict_n['安値'] = sql_pd['安値L1'][0]
            dict_n['終値'] = sql_pd['終値L1'][0]
        # 終値期限now取得
        sqls = "select *,SUBSTR(now,12,2) as T,rowid from %(table)s where rowid=(select max(rowid) from %(table)s)" % {'table': table_name}
        sql_pd = common.select_sql(DB_INFO, sqls)
        dict_e['now'] = sql_pd['now'][0]
        dict_e['始値'] = sql_pd['始値'][0]
        dict_e['高値'] = sql_pd['高値'][0]
        dict_e['安値'] = sql_pd['安値'][0]
        dict_e['終値'] = sql_pd['現値'][0]
        dict_e['買気'] = sql_pd['買気'][0]
        dict_e['売気'] = sql_pd['売気'][0]

        dict_e = common.to_number(dict_e)
        dict_d = common.to_number(dict_d)
        dict_n = common.to_number(dict_n)
        print(common.env_time()[0],dict_e, dict_d, dict_n)
        return dict_e, dict_d, dict_n

    def after_update(self):
        # 全テーブル情報取得
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(BYBY_DB, sqls)
        for i, rrow in sql_pd.iterrows():
            table_name = rrow['name']
            cnt = 0
            # ゴムは小数点まで
            if table_name.count("GOMU"):
                cnt = 1
            if table_name == 'GOLDday23':
                common.sum_clce(BYBY_DB, table_name, '決済16', '合計16', cnt)
                common.sum_clce(BYBY_DB, table_name, '決済23', '合計23', cnt)
            else:
                self.sum_clce(BYBY_DB, table_name, '買い決済', '買い合計', cnt, '買い玉')
                self.sum_clce(BYBY_DB, table_name, '売り決済', '売り合計', cnt, '売り玉')

    def sum_clce(self, DB, table, row1, t_sum, FX, real_cnt):

        sqls = "select %(key1)s,%(key2)s,%(key3)s,rowid from %(table)s" % {'table': table, 'key1': row1, 'key2': t_sum, 'key3': real_cnt}
        sql_pd = common.select_sql(DB, sqls)
        if len(sql_pd) < 2:
            return
        l_sum = 0
        if len(sql_pd) < 100:
            cnt = 0
        else:
            cnt = int(max(len(sql_pd) / 2, len(sql_pd)-500))
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            # リアル以外は値を０
            if row[real_cnt] != None:
                pass
            else:
                row[row1] = 0

            try:
                # FXは四捨五入の有無チェック
                if FX == None:
                    row[t_sum] = int(row[row1]) + int(l_sum)
                else:
                    row[t_sum] = round(row[row1] + l_sum, FX)
                l_sum = row[t_sum]
            except:
                row[t_sum] = l_sum
                l_sum = row[t_sum]
            if cnt <= i:
                sqls = "UPDATE %(table)s SET %(key3)s = '%(key1)s' where rowid = '%(key2)s'" % {'table': table, 'key1': row[t_sum], 'key2': row['rowid'], 'key3': t_sum}
                print(sqls)
                common.db_update(DB, sqls)

    def GOLD_16(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        table_name = 'GOLDday23'
        dict_w = {}
        dict_e, dict_d, dict_n = info.get_data(7, 'GOLD', '限月金')
        dict_d = self.tocom_info(4)  # 4は日中金
#        dict_n = self.tocom_info(2) #2は夜間金
        dict_w['朝決済'] = dict_d['始値']
        dict_w['始値N'] = dict_n['始値']
        dict_w['高値N'] = dict_n['高値']
        dict_w['安値N'] = dict_n['安値']
        dict_w['終値N'] = dict_n['終値']
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        # ////////////////////////////////////////////////// #
        # ---------------- ここに決済情報を入力する ---------------- #
        # ////////////////////////////////////////////////// #
        # 16時の仕掛確認
        if sql_pd['byby_type23'][0] is None:
            pass
        elif int(sql_pd['byby_type23'][0]) > 0:
            dict_w['決済23'] = dict_w['朝決済'] - int(sql_pd['終値23'][0])
            print("買い残あり")
        elif int(sql_pd['byby_type23'][0]) < 0:
            dict_w['決済23'] = int(sql_pd['終値23'][0]) - dict_w['朝決済']
            print("売り残あり")

        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行

        # ////////////////////////////////////////////////// #
        # ---------------- 16時からの仕掛け ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        dict_f = {}
        dict_w['始値D'] = dict_d['始値']
        dict_w['高値D'] = dict_d['高値']
        dict_w['安値D'] = dict_d['安値']
        dict_w['終値D'] = dict_d['終値']
        # VIX JPYなど
        dict_w.update(common.info_index())
        dict_w.update(common.bloomberg_real())
        sqls = "select *,rowid from %(table)s order by now desc;" % {'table': table_name}
        sql_pd3 = common.select_sql(BYBY_DB, sqls)
        sql_pd3 = common.to_number(sql_pd3)
        # フラグ1
        try:#緊急対応
            if float(sql_pd3['VIX'][0]) > dict_w['VIX']:
                dict_w['VIX1'] = -1
            else:
                dict_w['VIX1'] = 1
            t_max = sql_pd3.head(20)
            if float(t_max['USGG10YR'].max()) > dict_w['USGG10YR']:
                dict_w['USGG10YR20'] = -1
            else:
                dict_w['USGG10YR20'] = 1

            t_max = sql_pd3.head(5)
            if t_max['USDJPY'].max() > dict_w['USDJPY']:
                dict_w['USDJPY5'] = -1
            else:
                dict_w['USDJPY5'] = 1
            if dict_w['USDJPY5'] == -1 or dict_w['VIX1'] == -1:
                dict_f['FLAG16_1'] = -1
            else:
                dict_f['FLAG16_1'] = 0
        except:
            dict_f['FLAG16_1'] = 0
            pass

        # フラグ2
        if sql_pd3['始値23'][0] > sql_pd3['終値23'][0]:
            dict_f['FLAG16_2'] = -1
        elif sql_pd3['始値23'][1] < sql_pd3['終値23'][1]:  # dict_w['前始値'] < dict_w['前現値']の条件もあり
            dict_f['FLAG16_2'] = 1
        # フラグ3
        print(sql_pd3['終値23'][0], dict_w['安値D'])
        if int(sql_pd3['終値23'][0]) - dict_w['安値D'] > 50:
            dict_f['FLAG16_3'] = -1
        else:
            dict_f['FLAG16_3'] = 0
        # フラグ4
        if dict_w['始値D'] > dict_w['終値D'] and dict_w['始値D'] > int(sql_pd3['終値23'][0]):
            dict_f['FLAG16_4'] = -1
        else:
            dict_f['FLAG16_4'] = 0
        # フラグ5
        if int(sql_pd3['高値23'][0]) - int(sql_pd3['安値23'][0]) < dict_w['高値D'] - dict_w['安値D']:
            dict_f['FLAG16_5'] = -1
        else:
            dict_f['FLAG16_5'] = 0
        # フラグ6
        today = datetime.date.today()
        if today.weekday() == 0:
            dict_f['FLAG16_6'] = -1
        elif today.weekday() == 2:
            dict_f['FLAG16_6'] = 1
        else:
            dict_f['FLAG16_6'] = 0
        # フラグ7
        avg5 = sql_pd3['終値D'].rolling(5).mean().dropna()
        dict_w['FLAG16_avg5'] = avg5[4]
        # フラグ合計
        dict_w['flag_sum16'] = sum(list(dict_f.values()))
        if dict_w['flag_sum16'] > 0:
            dict_w['byby_type16'] = 1
            bybypara = {'code': '東京金', 'amount': self.gold_amount, 'buysell': '買','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き買い'}
            self.send_msg += bybypara['code'] + \
                "_" + bybypara['comment'] + "\n"
#            result, msg = c01_hokushin.hoku_main(bybypara)
#            self.send_msg += bybypara['code'] + "_" + msg + ":" + str(bybypara['amount']) + str(result) + "\n"
        elif dict_w['flag_sum16'] < 0 and dict_w['FLAG16_avg5'] > dict_w['終値D']:
            dict_w['byby_type16'] = -1
            bybypara = {'code': '東京金', 'amount': self.gold_amount, 'buysell': '売','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き売り'}
            self.send_msg += bybypara['code'] + "_" + bybypara['comment'] + "\n"
#            result, msg = c01_hokushin.hoku_main(bybypara)
#            self.send_msg += bybypara['code'] + "_" + msg + ":" + str(bybypara['amount']) + str(result) + "\n"
        else:
            dict_w['byby_type16'] = 0
        dict_w.update(dict_f)
        common.insertDB3(BYBY_DB, table_name, dict_w)
        return dict_w['byby_type16']

    def GOLD_23(self):
        # ////////////////////////////////////////////////// #
        # ---------------- データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        table_name = 'GOLDday23'
        dict_w = {}
        dict_f = {}
        dict_e, dict_d, dict_n = info.get_data(7, 'GOLD', '限月金')
        dict_w['始値23'] = dict_e['始値']
        dict_w['高値23'] = dict_e['高値']
        dict_w['安値23'] = dict_e['安値']
        dict_w['終値23'] = dict_e['終値']

        sqls = "select *,rowid from %(table)s order by now desc;" % {'table': table_name}
        sql_pd3 = common.select_sql(BYBY_DB, sqls)
        sql_pd3 = common.to_number(sql_pd3)
        # フラグ1
        cnt = 0
        for i in range(3):
            if int(sql_pd3['終値23'][i+1]) > int(sql_pd3['朝決済'][1+1]):
                cnt += 1
                if cnt == 3:
                    dict_f['FLAG23_1'] = 1
            if int(sql_pd3['終値23'][i+1]) < int(sql_pd3['朝決済'][1+1]):
                cnt -= 1
                if cnt == -3:
                    dict_f['FLAG23_1'] = -1
        # フラグ2
        if dict_w['始値23'] > dict_w['終値23']:
            dict_f['FLAG23_2'] = 1

        # フラグ3
        # 米国の金情報取得
        UURL = r"https://www.bloomberg.co.jp/markets/commodities/futures/metals"
        dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)

        for idx, row in dfs[0].iterrows():
            dict_w['GOLD23'] = row[2]
            break
        if float(dict_w['GOLD23']) - float(sql_pd3['GOLD23'][1]) > 20:
            dict_f['FLAG23_3'] = -1
        elif int(sql_pd3['USDJPY5'][0]) == 1 and int(sql_pd3['USGG10YR20'][0]) == 1:
            dict_f['FLAG23_3'] = 1

        # フラグ4
        if int(sql_pd3['終値D'][0]) > dict_w['終値23']:
            dict_f['FLAG23_4'] = 1

        # フラグ5
        today = datetime.date.today()
        if today.weekday() == 4:
            dict_f['FLAG23_5'] = 1

        # 16時の仕掛確認
        amount2_16 = int(sql_pd3['byby_type16'][0])
        print(sql_pd3['byby_type16'][0])
        if amount2_16 is None:
            amount2_16 = 0
            print("PASS")
            pass
        elif amount2_16 > 0:
            dict_w['決済16'] = dict_w['終値23'] - dict_w['始値23']
            print("買い残あり")
            self.send_msg += "_GOLD1_23_買い残あり_決済利益" + str(dict_w['決済16']) + "\n"
        elif amount2_16 < 0:
            dict_w['決済16'] = dict_w['始値23'] - dict_w['終値23']
            self.send_msg += "_GOLD1_23_売り残あり_決済利益" + str(dict_w['決済16']) + "\n"
            print("売り残あり")

        # フラグ合計
        dict_w['flag_sum23'] = sum(list(dict_f.values()))
        if dict_w['flag_sum23'] > 0:
            dict_w['byby_type23'] = 1
            amount_w = 1
        elif dict_w['flag_sum23'] < 0:
            dict_w['byby_type23'] = -1
            amount_w = -1
        else:
            dict_w['byby_type23'] = 0
            amount_w = 0
        if amount_w > 1:
            bybypara = {'code': '東京金', 'amount': amount_w, 'buysell': '買','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き買い'}
#            result, msg = c01_hokushin.hoku_main(bybypara)
#            self.send_msg += bybypara['code'] + "_" + msg + ":" + str(bybypara['amount']) + str(result) + "\n"
            self.send_msg += "_GOLD1_23_買い戦略" + str(bybypara['amount']) + "\n"
            amount = 1

        elif amount_w < 0:
            bybypara = {'code': '東京金', 'amount': abs(amount_w), 'buysell': '売', 'kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き売り'}
#            result, msg = c01_hokushin.hoku_main(bybypara)
#            self.send_msg += bybypara['code'] + "_" + msg + ":" + str(bybypara['amount']) + str(result) + "\n"
            self.send_msg += "_GOLD1_23_売り戦略" + str(bybypara['amount']) + "\n"
            amount = -1
        else:
            amount = 0
        dict_w.update(dict_f)
        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd3['rowid'][0]) #最後の引数を削除すると自動的に最後の行

        return amount,amount2_16

    def tocom_info(self, num):  # 金の行番号日中
        dict_w = {}
        # 信用残の推移(週次)
        UURL = "http://www.tocom.or.jp/jp/souba/souba_sx/index.html"
        # テーブル情報取得
        dfs = pd.read_html(UURL, header=0)
        col_list = list(dfs[num].columns)
        df = dfs[num].sort_index(ascending=False)  # ソート
        for idx, row in df.iterrows():
            for i in range(len(row)):
                if 1 < i < 6:
                    dict_w[col_list[i]] = row[i]
            return dict_w

    def GOLD2_16(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        table_name = 'GOLD2day24'
        dict_w = {}

        # ////////////////////////////////////////////////// #
        # ---------------- ここに決済情報を入力する ---------------- #
        # ////////////////////////////////////////////////// #
        dict_d = self.tocom_info(4)  # 4は日中金
#        dict_n = self.tocom_info(2) #2は夜間金
        dict_w['始値D'] = dict_d['始値']
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) and 決済24 = '1' ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        if len(sql_pd) > 0:
            print(dict_w['始値D'])
            print(sql_pd['現値24'][0])
            dict_w['決済24'] = int(dict_w['始値D']) - int(sql_pd['現値24'][0])
            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行

        # ////////////////////////////////////////////////// #
        # ---------------- 16時からの仕掛け ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        dict_w.update(common.info_index())
        dict_w.update(common.bloomberg_real())
        # 米国金価格取得
        DB_RASHIO = common.save_path('all_info.sqlite')
        sqls = "select 価格,rowid from %(table)s where 名称 = '%(key1)s'" % {'table': 'bloomberg_list', 'key1': 'GC1:COM 金 (CMX)'}
        sql_pd2 = common.select_sql(DB_RASHIO, sqls)
        numt = len(sql_pd2)-1
        dict_w['始値D'] = dict_d['始値']
        dict_w['終値D'] = dict_d['終値']
        dict_w['Vola_16'] = round(abs(float(sql_pd2.loc[numt, '価格']) / float(sql_pd2.loc[numt-1, '価格']))-1, 5)
        m = sql_pd2['価格'].rolling(5).mean()
        dict_w['usgold_avg5'] = round(float(sql_pd2.loc[numt, '価格'])/m[numt], 4)
        amount = 0
        if dict_w['Vola_16'] > 0 and dict_w['usgold_avg5'] < 1.01:
            amount = -1
            dict_w['決済16'] = '1'
            bybypara = {'code': '東京金', 'amount': self.gold_amount, 'buysell': '売','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き売り'}
#            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += "GOLD2_" + str(bybypara['comment']) + "\n"
            dict_w['amount_16'] = bybypara['amount']
        common.insertDB3(BYBY_DB, table_name, dict_w)
        return amount

    def GOLD2_24(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        table_name = 'GOLD2day24'
        dict_w = {}
        dict_e, dict_d, dict_n = info.get_data(7, 'GOLD', '限月金')

        # ////////////////////////////////////////////////// #
        # ---------------- ここに決済情報を入力する ---------------- #
        # ////////////////////////////////////////////////// #
        sqls = "select *,rowid from %(table)s" % {'table': 'GOLD'}
        sql_pd = common.select_sql(DB_INFO, sqls)
        cnt = len(sql_pd) - 1
        settle = 0
        dict_w['現値24'] = sql_pd.loc[cnt, '現値']
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd2 = common.select_sql(BYBY_DB, sqls)
        if len(sql_pd2) > 0:
            dict_w['仕掛値16'] = dict_e['始値']
            if str(sql_pd2['決済16'][0]) == '1':
                settle = -1
                dict_w['決済16'] = int(dict_w['現値24']) - int(dict_w['仕掛値16'])
                self.send_msg += "_GOLD2_24_売り残あり_決済利益" + str(dict_w['決済16']) + "\n"

#                sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd2['rowid'][0])

        # ////////////////////////////////////////////////// #
        # ---------------- 16時からの仕掛け ---------------- #
        # ////////////////////////////////////////////////// #
#        dict_w = {}
        # 終値期限now取得
        sqls = "select *,rowid from %(table)s" % {'table': 'GOLD'}
        sql_pd = common.select_sql(DB_INFO, sqls)
        cnt = len(sql_pd) - 1
        dict_w['終値_24'] = int(sql_pd.loc[cnt, '現値'])
        dict_w['始値_24'] = int(sql_pd.loc[cnt, '始値'])
        # USDJPY前日ボラ確認
        yest_day = str(datetime.date.today()).replace("-", "/") + ' 07:'
        sqls = "select *,rowid from %(table)s where now < '%(key1)s'" % {'table': 'gaitame_USDJPY', 'key1': yest_day}
        sql_pd2 = common.select_sql('fx.sqlite', sqls)
        numt = len(sql_pd2)-1
        dict_w['jpyusd07_0'] = float(sql_pd2.loc[numt, 'ask'])
        dict_w['jpyusd07_1'] = float(sql_pd2.loc[numt-95, 'ask'])
        dict_w['Vola_jpyusd24'] = round(dict_w['jpyusd07_0'] / dict_w['jpyusd07_1'], 5)

        # 夜仕掛け確認
        last_flag = 0
#        if dict_w['始値_24'] < dict_w['終値_24'] and dict_w['Vola_jpyusd24'] < 1:
        if dict_w['始値_24'] < dict_w['終値_24']:
            self.send_msg += "_GOLD2_24_買い戦略あり" + "\n"
            last_flag = 1
        # 仕掛け仕切り実行
        if last_flag > 0:
            dict_w['決済24'] = '1'
            bybypara = {'code': '東京金', 'amount': last_flag, 'buysell': '買い','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き買い'}
#            result, msg = c01_hokushin.hoku_main(bybypara)
#            self.send_msg += bybypara['code'] + "_" + msg + ":" + str(bybypara['amount']) + str(result) + "\n"
            dict_w['amount_24'] = bybypara['amount']
            self.send_msg += "GOLD2_" + str(bybypara['comment']) + "\n"
        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name)  #最後の引数を削除すると自動的に最後の行
        return last_flag,settle

    def priod_day(self, tsd, priod, col):
        tsd = tsd.set_index('now')
        df = pd.DataFrame(index=pd.date_range('2007/01/01', common.env_time()[1][0:10]))
        df = df.join(tsd)
        tsd = df.dropna()
        o = tsd[col[0]].resample(priod).first().dropna()
        h = tsd[col[1]].resample(priod).max().dropna()
        l = tsd[col[2]].resample(priod).min().dropna()
        c = tsd[col[3]].resample(priod).last().dropna()
        tsd2 = pd.concat([o, h, l, c], axis=1)
        tsd2.columns = col
        return tsd2

    def weekly_stg(self,table_d,table_name,code):

        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        list_w = ['S2_O', 'S2_H', 'S2_L', 'S2_C']
        sqls = "select SUBSTR(now,1,10) as now ,S2_O,S2_H,S2_L,S2_C from %(table)s" % {'table': table_d}
        sqls = "select SUBSTR(now,1,10) as now,始値 as S2_O,高値 as S2_H,安値 as S2_L,終値 as S2_C from %(table)s" % {'table': table_d}
        sql_pd = common.select_sql('all_info.sqlite', sqls)
        sql_pd = info.priod_day(sql_pd, 'W', list_w)

        num = len(sql_pd) - 1
        dict_w = {}
        dict_w[list_w[0]] = sql_pd[list_w[0]][num] #S2_O
        dict_w[list_w[1]] = sql_pd[list_w[1]][num] #S2_H
        dict_w[list_w[2]] = sql_pd[list_w[2]][num] #S2_L
        dict_w[list_w[3]] = sql_pd[list_w[3]][num] #S2_C
        # データ取得

        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd2 = common.select_sql(BYBY_DB, sqls)
        if len(sql_pd2) > 0:
            buy, sell = self.round2(code, sql_pd2['S1_H'][0], sql_pd2['S1_L'][0])
            if buy < int(sql_pd['S2_H'][num]):
                dict_w['LongPL'] = int(sql_pd['S2_O'][num]) - buy
                dict_w['LongSum'] = ""
                dict_w['LongPL'] = round(dict_w['LongPL'], self.unit(code))
                print("仕掛買い")
            if sell < int(sql_pd['S2_L'][num]):
                dict_w['ShortPL'] = sell - int(sql_pd['S2_O'][num])
                dict_w['ShortSum'] = ""
                dict_w['ShortPL'] = round(dict_w['ShortPL'], self.unit(code))
                print("仕掛売り")

            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd2['rowid'][0])

        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w2 = {}
        dict_w2["S1_O"] = sql_pd[list_w[0]][num] #S1_O
        dict_w2["S1_H"] = int(sql_pd[list_w[1]][num]) + 100 #S1_H
        dict_w2["S1_L"] = int(sql_pd[list_w[2]][num]) - 100 #S1_L
        dict_w2["S1_C"] = sql_pd[list_w[3]][num] #S1_C
        dict_w2["S0_C"] = sql_pd2['S1_C'][0]   #S0_C
        common.insertDB3(BYBY_DB, table_name, dict_w2)

    def WTI_W_EXEC(self,code,title):
        dict_e, dict_d, dict_n = info.get_data(7, 'WTI', '限月プラッツドバイ原油')
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) " % {'table': 'WTI_weekly'}
        sql_pd = common.select_sql(BYBY_DB, sqls)

        if title == 'night':
            S1_H = dict_d['高値']
            S1_L = dict_d['安値']
        if title == 'day':
            S1_H = dict_n['高値']
            S1_L = dict_n['安値']
        print(sql_pd['S1_H'][0] , S1_H , sql_pd['S1_H'][0] , dict_e['売気'])
        if sql_pd['S1_L'][0] < S1_L and sql_pd['S1_L'][0] < dict_e['買気']:
            bybypara = {'code': code, 'amount': 1, 'buysell': '売','kubun': '逆指値･成行', 'nari_hiki': '', 'settle': sql_pd['S1_L'][0], 'comment': '原油W売りSTL'}
            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += code + "_" + msg + ":逆差値_" + str(sql_pd['S1_L'][0]) + ":現値_" + str(dict_e['終値']) + "_now_" + str(dict_e['now']) + "\n"
        if sql_pd['S1_H'][0] > S1_H and sql_pd['S1_H'][0] > dict_e['売気']:
            bybypara = {'code': code, 'amount': 1, 'buysell': '買','kubun': '逆指値･成行', 'nari_hiki': '', 'settle': sql_pd['S1_H'][0], 'comment': '原油W買いSTL'}
            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += code + "_" + msg + ":逆差値_" + str(sql_pd['S1_H'][0]) + ":現値_" + str(dict_e['終値']) + "_now_" + str(dict_e['now']) + "\n"


    def pura_16(self):
        table_name = "PRA_16_22"
        # ////////////////////////////////////////////////// #
        # ---------------- 16時からの仕掛け ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        # リアル米国情報取得
        dict_w.update(common.bloomberg_real())

        dict_d = self.tocom_info(20)  # 20は日中白金
        dict_w['S0_O'] = dict_d['始値']
        dict_w['S0_C'] = dict_d['終値']
        common.insertDB3(BYBY_DB, table_name, dict_w)

        sqls = "select *,rowid from %(table)s" % {'table': table_name}
        sql_pd2 = common.select_sql(BYBY_DB, sqls)
        numt = len(sql_pd2) - 1

        if numt > 1:
            dict_w['プラチナ_Vola'] = round(float(sql_pd2.loc[numt, 'プラチナ米ドル']) / float(sql_pd2.loc[numt-1, 'プラチナ米ドル']), 5)

            m = sql_pd2['プラチナ米ドル'].rolling(5).mean()
            dict_w['プラチナ_avg5'] = round(float(sql_pd2.loc[numt, 'プラチナ米ドル'])/m[numt], 4)
            if dict_w['プラチナ_Vola'] < 1 and dict_w['プラチナ_avg5'] < 0.998:
                dict_w['memo'] = 'sell'
                bybypara = {'code': '東京白金', 'amount': 2, 'buysell': '売','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き売り'}
                result, msg = c01_hokushin.hoku_main(bybypara)
                self.send_msg += bybypara['code'] + "_" + msg + ":" + str(bybypara['amount']) + str(result) + "\n"
                dict_w['amount'] = bybypara['amount']
            print(dict_w)
            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd2['rowid'][numt])

    def pura_22(self):
        table_name = "PRA_16_22"
        # ////////////////////////////////////////////////// #
        # ---------------- 仕切り確認 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        # データ取得
        dict_e, dict_d, dict_n = self.get_data(7, 'pura', '限月白金')
        dict_w['S2N_O'] = dict_e['始値']
        dict_w['S2N_H'] = dict_e['高値']
        dict_w['S2N_L'] = dict_e['安値']
        dict_w['S2N_R22'] = dict_e['終値']

        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s)" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        if sql_pd['memo'][0] == 'sell':
            # 仕切りの戦略
            msg = c01_hokushin.hoku_settles('東京白金',"売")
            dict_w['PL'] = int(dict_w['S2N_O']) - int(dict_w['S2N_R22'])

        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])

    def GOLD_main(self, title):
        if title == "GOLD16":
            self.send_msg += "★★★GOLD_16★★★\n"
            amount = self.GOLD_16()
            self.send_msg += "★★★GOLD2_16★★★\n"
            amount += self.GOLD2_16()
        if title == "GOLD24":
            self.send_msg += "★★★GOLD_23★★★\n"
            amount1,amount1_16 = self.GOLD_23()
            self.send_msg += "★★★GOLD2_24★★★\n"
            amount2, amount2_16 = self.GOLD2_24()
            #仕切りチェック
            self.send_msg += "★★★GOLD2_仕切り★★★\n"
            amount = amount1 + amount2
            amount_16 = amount1_16 + amount2_16
            print("1",amount , amount_16)
            self.GOLD_check(amount, amount_16)
            if amount == amount_16:
                amount = 0
                amount_16 = 0
            elif amount > 0 and amount_16 > 0:
                amount -= 1
                amount_16 -= 1
            elif amount < 0 and amount_16 < 0:
                amount += 1
                amount_16 += 1

            if amount_16 > 0:
                msg = c01_hokushin.hoku_settles('東京金',"買")
                self.send_msg += title + "_" + msg + ":仕切り買い" + "\n"
            elif amount_16 < 0:
                msg = c01_hokushin.hoku_settles('東京金',"売")
                self.send_msg += title + "_" + msg + ":仕切り売り" + "\n"
            print("2",amount , amount_16)

        self.send_msg += "★★★GOLD_売買実行★★★\n"
        amount = self.gold_amount * amount

        if amount > 0:
            bybypara = {'code': '東京金', 'amount': amount, 'buysell': '買','kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き買い'}
            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += title + "_" + msg + ":" + str(bybypara['amount']) + "_" + str(result) + "\n"
            self.send_msg += "GOLD_買い戦略_" + str(bybypara['amount']) + "\n"

        elif amount < 0:
            bybypara = {'code': '東京金', 'amount': abs(amount), 'buysell': '売', 'kubun': '成行', 'nari_hiki': '', 'settle': 1, 'comment': '成り行き売り'}
            result, msg = c01_hokushin.hoku_main(bybypara)
            self.send_msg += title + "_" + msg + ":" + str(bybypara['amount']) + "_" + str(result) + "\n"
            self.send_msg += "GOLD_売り戦略_" + str(bybypara['amount']) + "\n"

    def GOLD_check(self, amount, amount_16):
        table_name = 'GOLD_check'
        dict_w = {}
        dict_e, dict_d, dict_n = info.get_data(7, 'GOLD', '限月金')
#        dict_n = self.tocom_info(2) #2は夜間金
        #夜間処理結果
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        if len(sql_pd) > 0:
            dict_w['S3_O'] = dict_d['始値']
            if sql_pd['amount'][0] > 0:
                dict_w['ShortPL'] = dict_w['S3_O'] - sql_pd['S2_R'][0]
            elif sql_pd['amount'][0] < 0:
                dict_w['ShortPL'] = (dict_w['S3_O'] - sql_pd['S2_R'][0]) * -1
            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行
        #24時処理
        dict_w = {}
        dict_w['S2_O'] = dict_e['始値']
        dict_w['S2_R'] = dict_e['終値']
        dict_w['amount'] = amount
        dict_w['amount_16'] = amount_16
        if dict_w['amount_16'] > 0:
            dict_w['LongPL'] = dict_w['S2_R'] - dict_w['S2_O']
        elif dict_w['amount_16'] < 0:
            dict_w['LongPL'] = (dict_w['S2_R'] - dict_w['S2_O']) * -1
        dict_w['LongSum'] = ""
        dict_w['ShortSum'] = ""
        common.insertDB3(BYBY_DB, table_name, dict_w)
        common.sum_clce(BYBY_DB, table_name, 'LongPL', 'LongSum', 0)
        common.sum_clce(BYBY_DB, table_name, 'ShortPL', 'ShortSum', 0)


if __name__ == '__main__':  # 土曜日は5 datetime.datetime.now().weekday()
    argvs = sys.argv
    info = C01_commdy_stg()
    if argvs[1] == "w_start_800":
        if common.week_start_day() == 1:  # 800位
            msg = c01_hokushin.hoku_settles('東京原油',"買")
    if argvs[1] == "day840":
        info.WTI_W_EXEC('東京原油','day')
        info.com_exec('GOMU', '東京ゴム', 0.2, 9, '限月ゴム', 6)
    if argvs[1] == "day10":
        info.com_exec('GOMU', '東京ゴム', 0.2, 10, '限月ゴム', 3)
        info.com_exec('pura', '東京白金', 2, 10, '限月白金', 4)
    if argvs[1] == "day11":
        info.com_exec('COOM', '東京コーン', 50, 11, '限月とうもろこし', 4)

    if argvs[1] == "day16":
        # フラグファイル削除
        flag_file = common.save_path(common.DROP_DIR, "COMM_*")
        files = glob.glob(flag_file)  # ワイルドカードが使用可能
        for file in files:
            os.remove(file)
        info.WTI_W_EXEC('東京原油','night')
        info.pura_16()
        info.GOLD_main("GOLD16")

    if argvs[1] == "day17":
        info.com_exec('GOLD', '東京金', 2, 17, '限月金', 3)
        info.com_exec('GOMU', '東京ゴム', 0.2, 17, '限月ゴム', 3)
        info.after_update()  # 全テーブル情報取得 損益合計計算

    if argvs[1] == "day22":
        info.pura_22()
    if argvs[1] == "day24":
        info.GOLD_main("GOLD24")

    if argvs[1] == "weekly":
        info.weekly_stg('限月プラッツドバイ原油','WTI_weekly','プラッツドバイ原油')
        info.weekly_stg('限月とうもろこし','COOM_weekly','東京コーン')

    common.mail_send(u'商品トレード', info.send_msg)
    print("end", __file__)
