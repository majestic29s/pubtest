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
        self.INFO_DB = common.save_path('B05_cfd_stg.sqlite')
    # 3つの移動平均を使った戦略

    def breakout_ma_three(self, window0, window9, window5, col,  table):
        status = 0
        data = {'L_flag': "", 'S_flag': "", 'S_PL': "", 'L_PL': "", 'S3_R': ""}
        sqls = 'select "%(key1)s" from %(table)s;' % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])
        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_three"
        common.insertDB3(self.INFO_DB, tablename, data)
        cnt = len(sql_pd) - 1

        data['avg_'+str(window0)] = sql_pd[col].rolling(window0).mean().shift(1)[cnt]
        data['avg_'+str(window9)] = sql_pd[col].rolling(window9).mean().shift(1)[cnt]
        data['avg_'+str(window5)] = sql_pd[col].rolling(window5).mean().shift(1)[cnt]

        # init----------------------------------
        sqls = "select *,rowid from %(table)s" % {'table': tablename}
        tsd = common.select_sql(self.INFO_DB, sqls)
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
        if S_flag == 0 and data['S3_R'] < data['avg_'+str(window0)] < data['avg_'+str(window5)] < data['avg_'+str(window9)]:
            data['S_flag'] = data['S3_R']
            status = -1
        elif (data['avg_'+str(window0)] > data['avg_'+str(window5)] or data['avg_'+str(window5)] > data['avg_'+str(window9)]) and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']
            data['S_flag'] = 0
            status = -2
        # entry short-position
        elif L_flag == 0 and data['S3_R'] > data['avg_'+str(window0)] > data['avg_'+str(window5)] > data['avg_'+str(window9)]:
            data['L_flag'] = data['S3_R']
            status = 1
        elif (data['avg_'+str(window0)] < data['avg_'+str(window5)] or data['avg_'+str(window5)] < data['avg_'+str(window9)]) and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag
            data['L_flag'] = 0
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        sqls = common.create_update_sql(self.INFO_DB, data, tablename, sql_pd['rowid'][0])

    # クロスオーバー移動平均
    def breakout_ma_two(self, window0, window9, col,  table):
        status = 0
        data = {'L_flag': "", 'S_flag': "", 'S_PL': "", 'L_PL': ""}
        sqls = "select %(key1)s from %(table)s;" % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])

        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_ma_two"
        common.insertDB3(self.INFO_DB, tablename, data)
        sqls = "select *,rowid from " + tablename
        tsd = common.select_sql(self.INFO_DB, sqls)
        cnt = len(sql_pd) - 1
        data['avg_'+str(window0)] = sql_pd[col].rolling(window0).mean().shift(1)[cnt]
        data['avg_'+str(window9)] = sql_pd[col].rolling(window9).mean().shift(1)[cnt]

        cnt2 = len(tsd) - 2
        if tsd.loc[cnt2, 'S_flag'] is None or tsd.loc[cnt2, 'S_flag'] == "":
            S_flag = 0
        else:
            S_flag = float(tsd.loc[cnt2, 'S_flag'])

        if tsd.loc[cnt2, 'L_flag'] is None or tsd.loc[cnt2, 'L_flag'] == "":
            L_flag = 0
        else:
            L_flag = float(tsd.loc[cnt2, 'L_flag'])

#        window0 = 32 797.98438
#        window9 = 12 798.41667

        data['S_flag'] = tsd.loc[cnt2, 'S_flag']
        data['L_flag'] = tsd.loc[cnt2, 'L_flag']
        common.to_number(data)
        status = 0
        #仕切り
        if data['avg_'+str(window0)] > data['avg_'+str(window9)] and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-data['S3_R']  # レポート用
            data['S_flag'] = 0
            status = -2
        elif data['avg_'+str(window0)] < data['avg_'+str(window9)] and L_flag != 0:  # exit short-position
            data['L_PL'] = data['S3_R']-L_flag  # レポート用
            data['L_flag'] = 0
        #仕掛け
        elif data['avg_'+str(window0)] < data['avg_'+str(window9)] and S_flag == 0:  # entry short-position
            data['S_flag'] = data['S3_R']
            status = -1
        elif data['avg_'+str(window0)] > data['avg_'+str(window9)] and L_flag == 0:  # entry short-position
            data['L_flag'] = data['S3_R']
            status = 1

#            status =  2
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        sqls = common.create_update_sql(self.INFO_DB, data, tablename, sql_pd['rowid'][0])
        return status

    # フィルター付き高値・安値のブレイクアウト
#        window0 = 2 window9 = 2 f0 = 12 f9 = 2
    def breakout_simple_f(self,  window0, window9, f0, f9, col,  table):
        status = 0
        data = {'L_flag': "", 'S_flag': "", 'L_SUM': "",'S_PL': "", 'L_PL': ""}
        sqls = "select %(key1)s from %(table)s;" % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['S3_R'] = float(sql_pd[col][-1:])
        # データ更新、データフレームに引き出す
        tablename = col + "_breakout_simple_f"
        common.insertDB3(self.INFO_DB, tablename, data)
        sqls = "select *,rowid from " + tablename
        tsd = common.select_sql(self.INFO_DB, sqls)
        cnt = len(sql_pd) - 1
        data['max_s' + str(window0)] = sql_pd[col].rolling(window0).max().shift(1)[cnt] #ub0
        data['min_s' + str(window0)] = sql_pd[col].rolling(window0).min().shift(1)[cnt] #2
        data['max_e' + str(window9)] = sql_pd[col].rolling(window9).max().shift(1)[cnt] #ub9
        data['min_e' + str(window9)] = sql_pd[col].rolling(window9).min().shift(1)[cnt] #lb9
        data['avg_l' + str(f0)] = sql_pd[col].rolling(f0).mean().shift(1)[cnt] #f0
        data['avg_s' + str(f9)] = sql_pd[col].rolling(f9).mean().shift(1)[cnt]  #f9
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
        c = data['S3_R']
        status = 0
        if c > data['max_e' + str(window9)] and S_flag != 0:  # exit short-position
            data['S_PL'] = S_flag-c  # レポート用
            data['S_flag'] = 0
            status = -2
        elif c < data['min_e' + str(window9)] and L_flag != 0:  # exit short-position
            data['L_PL'] = c-L_flag  # レポート用
            data['L_flag'] = 0
            status = 2
        elif c < data['min_s' + str(window0)] and S_flag == 0 and L_flag == 0 and data['avg_s' + str(f9)] > data['avg_l' + str(f0)]:
            data['S_flag'] = c
            status = -1
        elif c > data['max_s' + str(window0)] and S_flag == 0 and L_flag == 0 and data['avg_s' + str(f9)] < data['avg_l' + str(f0)]:
            data['L_flag'] = c
            status = 1
        """
        #仕切りチェック
        if status == -1 and L_flag != 0:
            print("仕切り1")
            self.byby_exec_fx(2, col, 1)
        if status == 1 and S_flag != 0:
            print("仕切り2")
            self.byby_exec_fx(-2, col, 1)
        """
        # rowid取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': tablename}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        sqls = common.create_update_sql(self.INFO_DB, data, tablename, sql_pd['rowid'][0])

        return status

    def breakout_ma_std(self, window, multi, para_key1, para_key2, col, table):
        data = {}
        # GMOFX情報取得
        stuts = 0
#        data = {'L_flag': "", 'S_flag': "", 'L_SUM': "",'S_PL': "", 'L_PL': ""}
        sqls = "select %(key1)s from %(table)s;" % {'table': table, 'key1': col}
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        data['S3_R'] = float(sql_pd.loc[0, col])
        tablename = col + "_breakout_ma_std"

        #前回失敗した場合は削除
        sqls = "delete from %(table)s where status IS NULL" % {'table':tablename}
        common.sql_exec(self.INFO_DB, sqls)

        common.insertDB3(self.INFO_DB, tablename, data)
        # データ更新、データフレームに引き出す
        sqls = "select * from " + tablename
        tsd = common.select_sql(self.INFO_DB, sqls)

        # 仕掛け処理更新
        tsd['ma_m'] = sql_pd[col].rolling(window).mean().dropna()
        tsd['ma_s'] = sql_pd[col].rolling(window).std()
        tsd['ub'] = tsd.ma_m+tsd.ma_s*multi
        tsd['lb'] = tsd.ma_m-tsd.ma_s*multi
        tsd['status'] = tsd['status'].shift(1)
        tsd['hold'] = tsd['hold'].shift(1)

        ind = datetime.datetime.now()
        cnt2 = len(tsd) - 1
        c = round(float(tsd['S3_R'][cnt2]), 4)
        ma_m = round(float(tsd.ma_m[cnt2]), 4)
        ma_s = round(float(tsd.ma_s[cnt2]), 4)
        ub = round(float(tsd.ub[cnt2]), 4)
        lb = round(float(tsd.lb[cnt2]), 4)
        L_PL = ""
        S_PL = ""
        status = float(tsd.status[cnt2])
        hold = tsd.hold[cnt2]
        try:
            hold = float(tsd.hold[cnt2])
        except:
            pass

        if c < lb and status == 0 and ind.hour >= para_key1 and ind.hour <= para_key2:  # entry short-position
            stuts = -1
            hold = c
            status = -1
            # 売り仕掛け

        if c > ma_m and status < 0:  # exit short-position
            stuts = -2
            S_PL = round(float(hold-c), 4)
            hold = ""
            status = 0
            # 売り仕切り

        if c > ub and status == 0 and ind.hour >= para_key1 and ind.hour <= para_key2:  # entry short-position
            stuts = 1
            hold = c
            status = 1
            # 買い仕掛け

        if c < ma_m and status > 0:  # exit short-position
            stuts = 2
            L_PL = round(float(c-hold), 4)
            hold = ""
            status = 0
            # 買い仕切り
        dict_w = {'ma_m': ma_m, 'ma_s': ma_s, 'ub': ub,'lb': lb, 'status': status, 'hold': hold, 'L_PL': L_PL, 'S_PL': S_PL}
        sqls = common.create_update_sql(self.INFO_DB, dict_w, tablename) #最後の引数を削除すると自動的に最後の行
        return stuts

    def stg_main(self):
        window0 = 10
        window9 = 3
        f0 = 70
        f9 = 80
        code = u'米NQ100'
        table = '_gmo_info'
        PL = self.breakout_simple_f(window0, window9, f0, f9, code, table)
        self.byby_exec_fx(PL, code, 1)


        window0 = 32
        window9 = 12
        code = u'米国VI'
        table = '_gmo_info'
        PL = self.breakout_ma_two(window0, window9, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 92
        window9 = 2
        code = u'コーン'
        table = '_gmo_info'
        PL = self.breakout_ma_two(window0, window9, code, table)
        self.byby_exec_fx(PL, code, 1)

        window0 = 82
        window9 = 2
        code = u'CADJPY'
        table = '_gmo_info'
        PL = self.breakout_ma_two(window0, window9, code, table)
        self.byby_exec_fx(PL, code, 2)

        window0 = 32
        window9 = 22
        code = u'AUDJPY'
        table = '_gmo_info'
        PL = self.breakout_ma_two(window0, window9, code, table)
        self.byby_exec_fx(PL, code, 2)

        window0 = 12
        window9 = 2
        f0 = 90
        f9 = 40
        code = 'GBPJPY'
        table = '_gmo_info'
        PL = self.breakout_simple_f(window0, window9, f0, f9, code, table)
        self.byby_exec_fx(PL, code, 2)

        window0 = 62
        window9 = 2
        f0 = 2
        f9 = 32
        code = '香港H'
        table = '_gmo_info'
        PL = self.breakout_simple_f(window0, window9, f0, f9, code, table)
        self.byby_exec_fx(PL, code, 1)

        window = 2
        multi = 4
        s_time = 0
        e_time = 24
        code = u'米30'
        table = '_gmo_info'
        PL = self.breakout_ma_std(window, multi, s_time, e_time, code, table)
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
        print("bybypara1",bybypara)
        if code.count("JPY") or code.count("USD"):
            result, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
        else:
            for iii in range(3):
                try:
                    result, msg = f02_gmo.gmo_cfd_exec(bybypara)
                    print(msg)
                    if msg.count('正常終了'):
                        break
                except:
                    pass
            else:
                bybypara['status'] = -5
                msg = '異常終了'
                common.insertDB3(self.INFO_DB, "retry", bybypara)

        self.send_msg += bybypara['comment'] + msg + "\n"
        return result

    def retry_check(self):
        sqls = 'select *,rowid from retry where status < 0 ;'
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            dict_w = {}
            bybypara = dict(row)
            try:
                result, msg = f02_gmo.gmo_cfd_exec(bybypara)
                if str(msg).count('正常終了'):
                    dict_w['status'] = 0
                else:
                    dict_w['status'] = row['status'] + 1
            except:
                dict_w['status'] = row['status'] + 1
                self.send_msg += u'CFDトレードリトライ異常終了_' + bybypara['code'] + "\n"

            sqls = common.create_update_sql(self.INFO_DB, dict_w, 'retry', bybypara['rowid']) #最後の引数を削除すると自動的に最後の行

    def main_test(self, HH):
        return 0
        code = u'米NQ100'
        if HH == 4:
            PL = 1 #買い
        elif HH == 5:
            PL = 2  #買い決済
        else:
            return 0
        self.byby_exec_fx(PL, code, 1)

    def cfd_poji_check(self):
        ok_msg = ""
        list_code, list_type = f02_gmo.info_pojicheck()
        codes, types, amounts = f03_ctfx.f03_ctfx_main({'kubun': 'ポジションチェック','amount':'2,000'})
        # 全テーブル情報取得
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(self.INFO_DB, sqls)
        for i, rrow in sql_pd.iterrows():
            table_name = rrow['name']
            sp_work = table_name.split("_")
            code = sp_work[0]
            if len(sp_work) != 4:
                continue
            sqls = "select L_flag,S_flag from %(table)s where (L_flag > 0 or S_flag > 0) and rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
            sql_pdd = common.select_sql(self.INFO_DB, sqls)
            print("AAA",sql_pdd)
            print("BBB",len(sql_pdd))
            if len(sql_pdd) > 0:
                type_w = ""
                if sql_pdd['L_flag'][0] != "":
                    if float(sql_pdd['L_flag'][0]) > 0:
                        type_w = "買"
                if sql_pdd['S_flag'][0] != "":
                    if float(sql_pdd['S_flag'][0]) > 0:
                        type_w = "売"
                if type_w != "":
                    #CDFのポジションチェック
                    for ii in range(len(list_code)):
                        if list_code[ii] == code and list_type[ii] == type_w:
                            del list_code[ii]
                            del list_type[ii]
                            ok_msg += u'CFDポジション一致_' + code +  "_" + type_w +"\n"
                            break
                    else:
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
                            if code.count("JPY") or code.count("USD"):
                                bybypara = {'code': code, 'amount': 2, 'buysell': type_w, 'kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': code + '_成行'}
    #                            f03_ctfx.f03_ctfx_main(bybypara)
                            else:
                                bybypara = {'code': code, 'amount': 1, 'buysell': type_w, 'kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': code + '_成行'}
    #                            f02_gmo.gmo_cfd_exec(bybypara)

        if len(list_code) > 0:
            self.send_msg += u'未決済銘柄あり_' + '_'.join([k for k in list_code]) + "\n" + '_'.join([k for k in list_type]) + "\n"
            for ii in range(len(list_code)):
                if list_code[ii] == '米S500': #米S500は除外
                    continue
                bybypara = {'code': list_code[ii], 'amount': 1, 'buysell': list_type[ii], 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': list_code[ii] + '_' + list_type[ii] + '決済'}
                f02_gmo.gmo_cfd_exec(bybypara)

        if len(codes) > 0:
            self.send_msg += u'未決済銘柄あり_' + '_'.join([k for k in codes]) + "\n" + '_'.join([k for k in types]) + "\n" + '_'.join([k for k in amounts]) + "\n" + ok_msg
            for ii in range(len(codes)):
                code = codes[ii][:3] + '/' + codes[ii][3:]
                bybypara = {'code': code, 'amount': amounts[ii][:1], 'buysell': types[ii], 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': codes[ii] + '_' + types[ii] + '決済'}
                f03_ctfx.f03_ctfx_main(bybypara)

    def main_TP(self, code='米NQ100', unit=300):  #トラリピ
        dict_ww = {}
        table_name = code[1:] + '_TP'
        #情報取得
        dict_w = f02_gmo.info_get()
        dict_w = common.to_number(dict_w)
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': '_gmo_info'}
        sql_pd = common.select_sql('B05_cfd_stg.sqlite', sqls)
        dict_t = sql_pd.to_dict('records')
        dict_t = common.to_number(dict_t[0])
        dict_ww['H'] = max(dict_w[code + '_高'],dict_t[code + '_高'])
        dict_ww['L'] = min(dict_w[code + '_安'],dict_t[code + '_安'])
        dict_ww['C'] = dict_w[code]
        #前日の仕掛け情報取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql('B05_cfd_stg.sqlite', sqls)
        if len(sql_pd) == 0:
            common.insertDB3('B05_cfd_stg.sqlite', table_name, dict_ww)
            return
        dict_l = sql_pd.to_dict('records')
        dict_l = common.to_number(dict_l[0])
        sp_work =[]
        #決済確認
        if dict_l.get('poji'):
            sp_work = common.to_number(str(dict_l['poji']).split("@"))
            for i in reversed(range(len(sp_work))):
                if dict_ww['C'] > sp_work[i] + unit:
                    # 買い決済
                    bybypara = {'code': code, 'amount': 1, 'buysell': '買', 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': code + '_買い決済'}
                    if code == '米S500':
                        print(bybypara)
                        try:#後でエラーある場合は落ちるように変更する。
                            result, msg = f02_gmo.gmo_cfd_exec(bybypara)
                            self.send_msg += '米S500_決算成功' + str(sp_work[i])
                        except:
                            self.send_msg += '米S500_決算失敗' + str(sp_work[i])
                        dict_ww['L_PL'] = dict_ww['C'] - sp_work[i]
                    else:
                        dict_ww['L_PL'] = dict_ww['C'] - sp_work[i]
                    sp_work.pop(i)
                    break
            dict_ww['poji'] = "@".join([str(n) for n in sp_work])
            print(dict_ww)
        #前日トレード確認
        if dict_l.get('trade'):
            poji = []
            sp_work2 = common.to_number(str(dict_l['trade']).split("@"))
            if len(sp_work2) > 0:
                for i in reversed(range(len(sp_work2))):
                    if  dict_ww['H'] > sp_work2[i]:
                        print("追加",sp_work2[i] , dict_ww['L'])
                        sp_work.append(sp_work2[i])
                        if code == '米S500':
                            try:
                                result, msg = f02_gmo.gmo_cfd_exec({'code': '米S500', 'kubun': 'ロスカット', 'settle': str(dict_ww['C'] - 500), 'comment': '米S500_ロスカット変更'})
                            except:
                                self.send_msg += '米S500_ロスカット変更失敗'

                dict_ww['poji'] = "@".join([str(n) for n in sp_work])

        #仕掛け
        if datetime.datetime.now().month < 8: #八月前は仕掛ける
            trade = []
            VAL = dict_ww['C'] - dict_ww['C'] % unit + unit
            for ii in range(3):
                VVAL = VAL + unit * ii
                if VVAL not in sp_work:
                    bybypara = {'code': code, 'amount': 1, 'buysell': '買', 'kubun': '新規逆指値', 'nari_hiki': '', 'settle': VVAL, 'comment': code + '_逆指値_買い'}
                    if code == '米S500':
                        print(bybypara)
                        for iii in range(5):
                            result, msg = f02_gmo.gmo_cfd_exec(bybypara)
                            list_order = f02_gmo.info_ordercheck()
                            if str(VVAL) not in list_order:
                                break
                        else:
                            common.mail_send(u'CFDトレード失敗', "SP500注文失敗" + str(VVAL))

                    trade.append(str(VVAL))
            dict_ww['trade'] = "@".join(trade)
            print(dict_ww)
        common.insertDB3('B05_cfd_stg.sqlite', table_name, dict_ww)

if __name__ == '__main__':  # 土曜日は5 datetime.datetime.now().weekday()
    info = e01_day_stg()
    argvs = sys.argv
    if argvs[1] == "daily_cfd": #4:50
        dict_w = f02_gmo.info_get()
        common.insertDB3('B05_cfd_stg.sqlite', '_gmo_info', dict_w)
        info.stg_main()
        info.cfd_poji_check()

    if argvs[1] == "toraripi": #700
        info.main_TP('米NQ100', 300)  #トラリピ
        info.main_TP('米S500',100)  #トラリピ

    hours = datetime.datetime.now().hour
    minutes = datetime.datetime.now().minute
    if argvs[1] == "retry_check" and minutes > 30:
        info.retry_check() #20190218
        if minutes > 40:
            info.main_test(hours)

    common.mail_send(u'CFDトレード', info.send_msg)

    print("end", __file__)
