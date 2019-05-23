import csv
import pandas as pd
import sys
import os
import datetime
import time
import numpy as np
from time import sleep
# 独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)
import k92_lvs
import k93_smbc

sys.path.append(common.PROFIT_DIR)
import common_profit as compf

STOCK_DB = common.save_path('B01_stock.sqlite')


class k04_ays_stg(object):
    def __init__(self):
        self.send_msg = ""

    def ATR_stg(self, code, year_s, year_e, title):
        code_text = os.path.join(compf.CODE_DIR, str(code) + '.txt')
        if os.path.exists(code_text):
            df = pd.DataFrame(index=pd.date_range(year_s + '/01/01', year_e + '/12/31'))
            df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
            df = df.dropna()
            df.columns = ['O', 'H', 'L', 'C', 'V', 'C2', 'SS'][:len(df.columns)]
            df = compf.add_avg_rng(df, 'C', 'L', 'H')
        else:
            print(code,code_text + "が存在しない")
            return pd.DataFrame({})

        y = df
        y['CL'] = y.C.shift(1)
        y['MA']= y.C.rolling(10).mean().shift(1)

        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N)  # 売りポジションの損益
        SumB = np.zeros(N)  # 売りポジションの損益
        SumS = np.zeros(N)  # 売りポジションの損益

        BSTL = np.empty(N)
        SSTL = np.empty(N)
        vora = np.empty(N)
        for i in range(10,len(y)-1):
            vora[i] = int((max(y.H[i],y.CL[i]) - min(y.L[i],y.CL[i])) * 0.85) + common.haba_type(y.CL[i])
            BSTL[i] = y.CL[i] + vora[i-1]
            SSTL[i] = y.CL[i] - vora[i-1]

            SumPL[i]=SumPL[i-1]
            SumB[i]=SumB[i-1]
            SumS[i]=SumS[i-1]
            if y.O[i] < BSTL[i] and BSTL[i] <= y.H[i] and y.MA[i] < y.C[i]:
                LongPL[i] = int((y.O[i+1] / BSTL[i] -1) * 1000000)
            if y.O[i] > SSTL[i] and SSTL[i] >= y.L[i] and y.MA[i] > y.C[i]:
                ShortPL[i] = int((SSTL[i] / y.O[i+1] -1) * 1000000)

            SumPL[i]=SumPL[i]+ShortPL[i]+LongPL[i] #レポート用
            SumB[i]=SumB[i]+LongPL[i] #レポート用
            SumS[i]=SumS[i]+ShortPL[i] #レポート用
        y['BSTL']= BSTL
        y['SSTL']= SSTL
        y['vora']= vora
        y['plb'] = LongPL
        y['pls'] = ShortPL
        y['sumB'] = SumB
        y['sumS'] = SumS
        y['sum'] = SumPL

        return y


    def STR_C(self):
        year_e = int(common.env_time()[0][:4])
        #カラムの初期化
        sqls = "update kabu_list set L_PL_085 = NULL ,S_PL_085 = NULL"
        common.sql_exec('B01_stock.sqlite', sqls)

        files = os.listdir(compf.CODE_DIR)
        for i in files:
            code = i.replace(".txt", "")
            try:
                y = self.ATR_stg(code, str(year_e - 4), str(year_e), "_base_" + str(year_e))
            except:
                print(code,"不明なエラー発生")
                continue
            if len(y) > 500 and int(y.O[1]) > 150 and common.stock_req(code, "SHELL") == 1:
                dict_pl = {}
                dict_w = {}
                L_PL = compf.check_PL(y['plb'])
                L_PL['MEMO'] = "L_PL_085"
                S_PL = compf.check_PL(y['pls'])
                S_PL['MEMO'] = "S_PL_085"
                for T_PL in [L_PL, S_PL]:
                    dict_w = {}
                    title = code + "_" + str(year_e) + T_PL['MEMO']
                    if T_PL['MEMO'] == "L_PL_085":
                        pl = 'plb'
                    if T_PL['MEMO'] == "S_PL_085":
                        pl = 'pls'

                    #5年間の成績
                    if (T_PL['WIN'] > 58 and T_PL['PL'] > 1.1 and T_PL['SUM_MAX_COMP'] > 3):
                        dict_pl.update(T_PL)
                        #前年の成績
                        y = self.ATR_stg(code, str(year_e - 0), str(year_e), "_" + title + "_" + str(year_e))
                        if len(y) == 0:
                            continue
                        T_PL = compf.check_PL(y[pl])
                        #前年と5年前を比較して前年の方が大きいこと
                        if (dict_pl['PL'] < T_PL['PL']):

                            dict_w[dict_pl['MEMO']] = dict_pl['WIN']
                            sqls = common.create_update_sql('B01_stock.sqlite', dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行


    def STR_MON(self):
        year_e = int(common.env_time()[0][:4])
        #カラムの初期化
#        sqls = "update kabu_list set L_PL_MON = NULL"
#        common.sql_exec('B01_stock.sqlite', sqls)

        files = os.listdir(compf.CODE_DIR)
        for i in files:
            code = i.replace(".txt", "")
            try:
                y = self.Monthly_last(code, str(year_e - 4), str(year_e), "_base_" + str(year_e))
            except:
                print(code,"不明なエラー発生")
                continue

            if len(y) > 500 and int(y.O[1]) > 150 and common.stock_req(code) == 0:
                dict_pl = {}
                dict_w = {}
                L_PL = compf.check_PL(y['plb'])
                L_PL['MEMO'] = "L_PL_MON"
                S_PL = compf.check_PL(y['pls'])
                S_PL['MEMO'] = "S_PL_MON"
                for T_PL in [L_PL, S_PL]:
                    dict_w = {}
                    title = code + "_" + str(year_e) + T_PL['MEMO']
                    if T_PL['MEMO'] == "L_PL_MON":
                        pl = 'plb'
                    if T_PL['MEMO'] == "S_PL_MON":
                        pl = 'pls'

                    #5年間の成績
                    if (T_PL['WIN'] > 65 and T_PL['PL'] > 1.5):
                        dict_pl.update(T_PL)
                        #前年の成績
                        y = self.Monthly_last(code, str(year_e - 1), str(year_e), "_" + title + "_" + str(year_e))
                        if len(y) == 0:
                            continue
                        T_PL = compf.check_PL(y[pl])
                        #前年と5年前を比較して前年の方が大きいこと
                        if (dict_pl['PL'] < T_PL['PL']):

                            dict_w[dict_pl['MEMO']] = dict_pl['WIN']
                            sqls = common.create_update_sql('B01_stock.sqlite', dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行

    def Monthly_last(self, code, year_s, year_e, title):
        code_text = os.path.join(compf.CODE_DIR, str(code) + '.txt')
        if os.path.exists(code_text):
            df = pd.DataFrame(index=pd.date_range(year_s + '/01/01', year_e + '/12/31'))
            df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
            df = df.dropna()
            df.columns = ['O', 'H', 'L', 'C', 'V', 'C2', 'SS'][:len(df.columns)]
            df = compf.add_avg_rng(df,'C','L','H')
            y = df
            y['O1']=(y.O / y.O.shift(1)-1) * 1000000
            y['O2']=(y.O / y.O.shift(2)-1) * 1000000
            y['O3']=(y.O / y.O.shift(3)-1) * 1000000
            y['O4']=(y.O / y.O.shift(4)-1) * 1000000
        else:
            print(code,code_text + "が存在しない")
            return pd.DataFrame({})

        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N)  # 売りポジションの損益
        SumB = np.zeros(N)  # 売りポジションの損益
        SumS = np.zeros(N)  # 売りポジションの損益
        for i in range(60,len(y)-1):
            SumB[i]=SumB[i-1]
            SumS[i]=SumS[i-1]
            if y.index[i].month != y.index[i+1].month:
                if float(y.rng30[i-1]) > 0.7:
                    LongPL[i] = y.O4[i]
                if float(y.rng30[i-1]) < 0.3:
                    ShortPL[i] = y.O4[i] * -1
            SumPL[i]=SumPL[i]+ShortPL[i]+LongPL[i] #レポート用
            SumB[i]=SumB[i]+LongPL[i] #レポート用
            SumS[i]=SumS[i]+ShortPL[i] #レポート用
        y['plb'] = LongPL
        y['pls'] = ShortPL
        y['sumB'] = SumB
        y['sumS'] = SumS
        y['sum'] = SumPL
#        save_name = os.path.join(self.S_DIR, str(code)  + "_Monthly_last_detail.csv")
#        y.to_csv(save_name)
        return y
    def L_MON_STG(self):
        if common.yutai_day(5) != common.env_time()[1][:10]:
            return

        title = '月末買い_stg'
        browser = ""
        sqls = "select *,rowid from %(table)s where %(key1)s IS NOT NULL and HighLow365 > 0.7 order by %(key1)s desc" % {'table': 'kabu_list','key1': 'L_PL_MON'}
        sql_pd = common.select_sql('B01_stock.sqlite', sqls)
        cnt = 0
        for i, row in sql_pd.iterrows():
            if i > 30:
                return
            if cnt > 10:
                title = '月末買い_stg_10以下'
            code = row['コード']

            arry = np.array([row['株価'], row['DAY1'], row['DAY1'], row['DAY2'], row['DAY3'], row['DAY4'], row['DAY5'], row['DAY6']])
            max7 = np.max(arry)
            min7 = np.min(arry)
            rng7 = round((arry[0] - min7) / (max7 - min7),2)
            if rng7 > 0.7:
                cnt += 1
                bybypara = {'code': code, 'amount': '100', 'buysell': u'買', 'kubun': '1','nari_hiki': u'成行', 'settle': 0, "comment": title, 'now': ''}
                bybypara["amount"] = common.ceil(code, 400000)  # IPO高値
                tsd = common.kabu_search(code)
                bybypara["kubun"] = tsd['貸借区分']
                result, msg, browser = k92_lvs.live_main(bybypara, browser)
                if result != 0:
                    self.send_msg += title + "_" + tsd['銘柄名'] + msg + "\n"

                # 売買履歴DBインポート
                end_day = common.yutai_day(1)
                bybyhist = {"日付": common.env_time()[1], "タイトル": title, "コード": bybypara["code"], "銘柄名": tsd['銘柄名'], "type": bybypara["buysell"],
                            "損切り幅": 0.1, "信用": bybypara["kubun"], "日数": 3, "玉": bybypara["amount"], "仕切り期限日": end_day, "memo": rng7}
                bybyhist = common.add_dict(code, bybyhist)
                common.insertDB3('B01_stock.sqlite', 'bybyhist', bybyhist)
                flag_file = common.save_path(common.DROP_DIR, str(code) + "_" + tsd['銘柄名'] + "_" + title + "_" + str(bybypara["amount"]))
                common.create_file(flag_file, str(code))
                print(code,tsd['銘柄名'])
        if self.send_msg == "":
            self.send_msg = title + "対象銘柄が存在しません。"

    def S_MON_STG(self):
        if common.yutai_day(5) != common.env_time()[1][:10]:
            return

        title = '月末売り_stg'
        browser = ""
        sqls = "select *,rowid from %(table)s where %(key1)s IS NOT NULL and HighLow365 < 0.5 and 貸借区分 = 1 order by %(key1)s desc" % {'table': 'kabu_list','key1': 'S_PL_MON'}
        sql_pd = common.select_sql('B01_stock.sqlite', sqls)
        cnt = 0
        for i, row in sql_pd.iterrows():
            if i > 30:
                return
            if cnt > 10:
                title = '月末売り_stg_10以下'
            code = row['コード']

            arry = np.array([row['株価'], row['DAY1'], row['DAY1'], row['DAY2'], row['DAY3'], row['DAY4'], row['DAY5'], row['DAY6']])
            max7 = np.max(arry)
            min7 = np.min(arry)
            rng7 = round((arry[0] - min7) / (max7 - min7),2)
            if rng7 < 0.3:
                cnt += 1
                bybypara = {'code': code, 'amount': '100', 'buysell': u'売', 'kubun': '1','nari_hiki': u'成行', 'settle': 0, "comment": title, 'now': ''}
                bybypara["amount"] = common.ceil(code, 400000)  # IPO高値
                tsd = common.kabu_search(code)
                bybypara["kubun"] = tsd['貸借区分']
                result, msg, browser = k92_lvs.live_main(bybypara, browser)
                if result != 0:
                    self.send_msg += title + "_" + tsd['銘柄名'] + msg + "\n"

                # 売買履歴DBインポート
                end_day = common.yutai_day(1)
                bybyhist = {"日付": common.env_time()[1], "タイトル": title, "コード": bybypara["code"], "銘柄名": tsd['銘柄名'], "type": bybypara["buysell"],
                            "損切り幅": 0.1, "信用": bybypara["kubun"], "日数": 3, "玉": bybypara["amount"], "仕切り期限日": end_day, "memo": rng7}
                bybyhist = common.add_dict(code, bybyhist)
                common.insertDB3('B01_stock.sqlite', 'bybyhist', bybyhist)
                flag_file = common.save_path(common.DROP_DIR, str(code) + "_" + tsd['銘柄名'] + "_" + title + "_" + str(bybypara["amount"]))
                common.create_file(flag_file, str(code))
                print(code,tsd['銘柄名'])
        if self.send_msg == "":
            self.send_msg = title + "対象銘柄が存在しません。"

    def ATR_STL_85_Hikenari(self, Title_w):
        browser = ""
        sqls = "select *,rowid from %(table)s where %(key1)s IS NOT NULL order by %(key2)s desc" % {'table': 'kabu_list','key1': Title_w,'key2': '変動率90'}
        sql_pd = common.select_sql('B01_stock.sqlite', sqls)
        common.to_number(sql_pd)
        for i, row in sql_pd.iterrows():
            if i > 30:
                return
            dict_w = {}
            code = row['コード']
            if common.stock_req(code, "SHELL") != 1:
                continue
            if float(row['変動率90']) < 1:
                continue
            yahoo = common.real_stock2(code)
            if yahoo['LastDay'] == 0:
                continue
            para = 0.85
            dict_w['vora'] = int((max(row['前日高値'],row['DAY1']) - min(row['前日安値'],row['DAY1'])) * para)
            dict_w['BSTL'] = yahoo['LastDay'] + dict_w['vora']
            dict_w['SSTL'] = yahoo['LastDay'] - dict_w['vora']

            avg10 = np.average(np.array([yahoo['price'],row['株価'], row['DAY1'], row['DAY1'], row['DAY2'], row['DAY3'], row['DAY4'], row['DAY5'], row['DAY6'], row['DAY7'], row['DAY8']]))
            if Title_w[:1] == 'L' and row['株価'] > avg10 and dict_w['BSTL'] < yahoo['price'] :
                if i > 15:
                    Title = Title_w + "_HIKE_10以下"
                else:
                    Title = Title_w + "_HIKE"
                bybyhist = {"日付": common.env_time()[1], "タイトル": Title, "コード": code, "銘柄名": yahoo['name'], "type": "買",
                            "損切り幅":  dict_w['BSTL'], "信用": '1' , "日数": 0, "玉": common.ceil(code, 400000), "仕掛け値": yahoo['price'], "終了日付": 4, "rank": i}
                bybyhist = common.add_dict(code, bybyhist)
                common.insertDB3('B01_stock.sqlite', 'bybyhist', bybyhist)
                bybypara = {'code': code, 'amount': bybyhist['玉'], 'buysell': bybyhist['type'], 'kubun': bybyhist['信用'],'nari_hiki': '引成', 'settle': 0, 'comment': bybyhist['タイトル']}
                result, msg, browser = k92_lvs.live_main(bybypara, browser)
            if Title_w[:1] == 'S' and row['株価'] < avg10 and dict_w['SSTL'] > yahoo['price']:
                if i > 15:
                    Title = Title_w + "_HIKE_10以下"
                else:
                    Title = Title_w + "_HIKE"
                bybyhist = {"日付": common.env_time()[1], "タイトル": Title, "コード": code, "銘柄名": yahoo['name'], "type": "売",
                            "損切り幅": dict_w['SSTL'], "信用": '1' , "日数": 0, "玉": common.ceil(code, 400000), "仕掛け値": yahoo['price'], "終了日付": 5, "rank": i}
                bybyhist = common.add_dict(code, bybyhist)
                common.insertDB3('B01_stock.sqlite', 'bybyhist', bybyhist)
                bybypara = {'code': code, 'amount': bybyhist['玉'], 'buysell': bybyhist['type'], 'kubun': bybyhist['信用'],'nari_hiki': '引成', 'settle': 0, 'comment': bybyhist['タイトル']}
                result, msg, browser = k92_lvs.live_main(bybypara, browser)

    def ATR_STL_85_Hikenari_update(self):
        sqls = "select *,rowid from bybyhist where 終了日付 in ('S_PL_085_HIKE','S_PL_085_HIKE','S_PL_085_HIKE_10以下','S_PL_085_HIKE_10以下')"
        sql_pd = common.select_sql(STOCK_DB, sqls)
        if len(sql_pd) > 0:
            for i, row in sql_pd.iterrows():
                code = row['コード']
                dict_w = {}
                yahoo = common.real_stock2(code)
                dict_w['memo'] = row['仕掛け値']
                dict_w['仕掛け値'] = yahoo['price']
                common.create_update_sql('B01_stock.sqlite', dict_w, 'bybyhist', row['rowid'])

if __name__ == '__main__':
    info = k04_ays_stg()
    argvs = sys.argv
    today = datetime.date.today()
    if "analyz_exe" == argvs[1]:  # 1900
        #年次メンテナンス
        year, month, day = common.env_time()[1][:10].split("/")
        if int(month) == 12 and int(day) > 20 and common.week_end_day() == 1:
            info.STR_C()
            info.STR_MON()
    elif "analyz_1500" == argvs[1]:  # 1500
        info.ATR_STL_85_Hikenari("S_PL_085")  #仮登録
        info.ATR_STL_85_Hikenari("L_PL_085")  #仮登録
        sleep(200)
        info.ATR_STL_85_Hikenari_update()

    elif "analyz_exe_m" == argvs[1]:  # 700
        info.L_MON_STG()
        info.S_MON_STG()
    else:
        info.send_msg = "引数が存在しません。:" + argvs[1]

    common.mail_send(u'アナライズトレード_' + argvs[1], info.send_msg)

    print("end", __file__)
