import pandas as pd
import os
import datetime
import time
from pandas.io import sql
import sys

import common
sys.path.append(common.LIB_DIR)
import f02_gmo
import f03_ctfx

FXBYBY_DB = common.save_path('fx_byby.sqlite')
FX_INFO = common.save_path('fx.sqlite')


class F01_day_stgFX(object):
    def __init__(self):
        self.row_arry = []
        self.send_msg = ""

    def main_fx(self):
        t = datetime.datetime.now()
        i = int(t.strftime("%H%M"))
        # 決済
        bybypara = {'code': 0}
        if 959 == i:
            bybypara = {'code': 'EUR/USD', 'amount': 7, 'buysell': '買', 'kubun': '決済','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx決済', 'now': '0'}
        if 1209 == i:
            bybypara = {'code': 'EUR/USD', 'amount': 7, 'buysell': '売', 'kubun': '決済','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx決済', 'now': '0'}
        if 2059 == i:
            bybypara = {'code': 'EUR/USD', 'amount': 7, 'buysell': '買', 'kubun': '決済','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx決済', 'now': '0'}
        if 759 == i:
            bybypara = {'code': 'EUR/JPY', 'amount': 7, 'buysell': '売', 'kubun': '決済','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx決済', 'now': '0'}
        if 1459 == i:
            bybypara = {'code': 'USD/JPY', 'amount': 10, 'buysell': '買', 'kubun': '決済','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx決済', 'now': '0'}
        if bybypara['code'] != 0:
            # 発注実行
            times = f02_gmo.gmo_fx_main(bybypara)
            # 仕掛値取得
            sqls = "select rowid,コード,type,仕掛値,玉 from bybyhist where 終了日 = '' and コード = '%(key1)s' " % {'key1': bybypara["code"]}
            sql_pd = common.select_sql(FXBYBY_DB, sqls)
            for i, row in sql_pd.iterrows():
                common.to_number(row)
                code = row['コード']
                # 通貨環境セット
                if code == 'EUR/USD':
                    amount_t = 1000000
                else:
                    amount_t = 10000
                buffer = 600 * row['玉']
                # 現在値取得
                now_data = float(common.real_info(code))
                for ii, rrow in sql_pd.iterrows():
                    S_data = float(rrow['仕掛値'])
                # 損益計算
                if row['type'] == "買":
                    profit = (S_data - now_data) * row['玉'] * amount_t - buffer
                if row['type'] == "売":
                    profit = (now_data - S_data) * row['玉'] * amount_t - buffer
                # 売買DB更新
                dict = {'table': 'bybyhist', 'key1': row['rowid'], 'key2': common.env_time()[1], 'key3': now_data, 'key4': int(profit)}
                sqls = "UPDATE %(table)s SET 終了日 = '%(key2)s', 決済値 = '%(key3)s',損益 = '%(key4)s' where rowid = '%(key1)s'" % dict
                common.db_update(FXBYBY_DB, sqls)
                self.send_msg += bybypara['comment'] + "_" + str(bybypara['code']) + "_" + bybypara['buysell'] + "_" + str(bybypara['amount']) + "\n"
                common.sum_clce(FXBYBY_DB, 'bybyhist', '損益', '合計')

        # 新規
        bybypara['code'] = 0
        if 759 == i:
            bybypara = {'code': 'EUR/USD', 'amount': 7, 'buysell': '売', 'kubun': '新規','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx新規', 'now': '0'}
        if 1034 == i:
            bybypara = {'code': 'EUR/USD', 'amount': 7, 'buysell': '買', 'kubun': '新規','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx新規', 'now': '0'}
        if 1654 == i:
            bybypara = {'code': 'EUR/USD', 'amount': 7, 'buysell': '売', 'kubun': '新規','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx新規', 'now': '0'}
        if 359 == i and t.weekday() != 0:
            bybypara = {'code': 'EUR/JPY', 'amount': 7, 'buysell': '買', 'kubun': '新規','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx新規', 'now': '0'}
        if 954 == i:
            bybypara = {'code': 'USD/JPY', 'amount': 10, 'buysell': '売', 'kubun': '新規','nari_hiki': '0', 'settle': '', 'comment': 'gmo_fx新規', 'now': '0'}
        if bybypara['code'] != 0:
            # 発注実行
            times = f02_gmo.gmo_fx_main(bybypara)
            # 仕掛値取得
            now_data = common.real_info(bybypara["code"])
            # 売買DBインポート
            bybyhist = {"日付": "", "タイトル": bybypara['comment'], "コード": bybypara["code"], "type": bybypara["buysell"],
                        "損切り幅": 0, "日数": 60, "玉": bybypara["amount"], "仕掛値": now_data, "決済値": "", "終了日": "", "損益": "", "合計": "", "memo": ""}
            common.insertDB3('fx_byby.sqlite', 'bybyhist', bybyhist)
            self.send_msg += bybypara['comment'] + "_" + str(bybypara['code']) + "_" + bybypara['buysell'] + "_" + str(bybypara['amount']) + "\n"

    def fx_weekly(self):
        yest_day = str(datetime.date.today() - datetime.timedelta(days=7)).replace("-", "/")
        AA = ["03", "04"]
        BB = ["EURUSD", "GBPJPY"]
        for i in range(len(AA)):
            table_name = BB[i] + "_Weekly"
            UURL = "https://info.ctfx.jp/service/market/csv/" + AA[i] + "_" + BB[i] + "_D.csv"
            dfs = pd.read_csv(UURL, header=0, encoding="cp932")
            df = dfs[dfs['日付'] >= str(yest_day)]
            dict_w = {}
            dict_w['日付'] = df['日付'].loc[0]
            dict_w['始値'] = str(round(float(df['始値'].loc[len(df)-1]), 4))
            dict_w['高値'] = str(round(float(df['高値'].max()), 4))
            dict_w['安値'] = str(round(float(df['安値'].min()), 4))
            dict_w['終値'] = str(round(float(df['終値'].loc[0]), 4))
            rid = common.last_rowid(FXBYBY_DB, table_name)
            sqls = common.create_update_sql(FXBYBY_DB, dict_w, table_name, rid)
#            common.insertDB3(FXBYBY_DB,table_name,dict_w) ＃全部インポートする場合はこちら

    def weekly_exec(self, range_W='Today'):
        AA = ["EUR/USD", "GBP/JPY"]
        BB = [5, 3]  # 切り捨て
        CC = [0.01, 3]  # 損切
        DD = [20, 50]  # 取引量
        EE = [0.75, 1]  # 仕掛け値
        for i in range(len(AA)):
            code = AA[i].replace("/", "")
            table_name = code + "_Weekly"
            sqls = "select *,rowid from " + table_name
            sql_pd = common.select_sql(FXBYBY_DB, sqls)
            # 全部計算する場合はToday以外を設定
            if range_W == 'Today':
                dfs = sql_pd[-3:]
            else:
                dfs = sql_pd
            dict_w = {}
            last_C = 0
            last_C1 = 0
            for ii, row in dfs.iterrows():
                row = common.to_number(row)
                if last_C == 0:
                    last_C = row['終値']
                    continue
                # 前日の値でインポート
                if len(dict_w) > 0:
                    if row['高値'] == None:
                        break
                    if row['高値'] > dict_w['買いSTL']:
                        dict_w['買い決済'] = round(row['終値'] - dict_w['買いSTL'], BB[i])
                        # 買い決済
                        bybypara = {'code': AA[i], 'amount': DD[i], 'buysell': '買', 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': AA[i]+'FX週次STL買い決済'}
                        result, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
                        self.send_msg += msg + "\n"
                    if row['安値'] < dict_w['売りSTL']:
                        dict_w['売り決済'] = round(dict_w['売りSTL'] - row['終値'], BB[i])
                        # 売り決済
                        bybypara = {'code': AA[i], 'amount': DD[i], 'buysell': '売', 'kubun': '決済','nari_hiki': '', 'settle': -1, 'comment': AA[i]+'FX週次STL売り決済'}
                        result, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
                        self.send_msg += msg + "\n"
                    # DBアップデート
                    sqls = common.create_update_sql(FXBYBY_DB, dict_w, table_name, row['rowid'])

                # 翌日のデータ作成
                dict_w = {}
                MAX_W = max(row['高値'], last_C)
                MIN_W = min(row['安値'], last_C)
                dict_w['変動幅'] = round(MAX_W - MIN_W, BB[i])
                dict_w['幅85'] = round(dict_w['変動幅'] * EE[i], BB[i])
                dict_w['買いSTL'] = round(dict_w['幅85']+row['終値'], BB[i])
                dict_w['売りSTL'] = round(row['終値']-dict_w['幅85'], BB[i])
                last_C1 = last_C
                last_C = row['終値']
                dict_w['vora'] = dict_w['幅85'] / row['終値']
            if range_W == 'Today':
                common.insertDB3(FXBYBY_DB, table_name, dict_w)
                # 買い売りしかけ
                if dict_w['vora'] < 0.03:
                    bybypara = {'code': AA[i], 'amount': DD[i], 'buysell': '売', 'kubun': '新規','nari_hiki': dict_w['売りSTL'] + CC[i], 'settle': dict_w['売りSTL'], 'comment': AA[i]+'FX週次STL売り'}
                    result, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
                    self.send_msg += msg + "\n"
                if dict_w['vora'] < 0.02:
                    bybypara = {'code': AA[i], 'amount': DD[i], 'buysell': '買', 'kubun': '新規','nari_hiki': dict_w['買いSTL'] - CC[i], 'settle': dict_w['買いSTL'], 'comment': AA[i]+'FX週次STL買い'}
                    result, msg, browser = f03_ctfx.f03_ctfx_main(bybypara)
                    self.send_msg += msg + "\n"
            common.sum_clce(FXBYBY_DB, table_name, '買い決済', '買い合計', BB[i])
            common.sum_clce(FXBYBY_DB, table_name, '売り決済', '売り合計', BB[i])

    def poji_check_main(self):
        result = ""
        # 全テーブル情報取得
        table_name = []
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(FXBYBY_DB, sqls)
        for i, rrow in sql_pd.iterrows():
            if rrow['name'].count('bybyhist') or rrow['name'].count('Weekly'):
                pass
            else:
                table_name.append(rrow['name'])
        # ポジションチェック
        dict_w = f02_gmo.poji_check_real()
#        dict_w = {'GBP/JPY':'売','AUD/USD':'買'}
        for k, v in dict_w.items():
            code = k
            print(code)
            for table in table_name:
                if table.count(code):
                    sqls = "select max(rowid),pl from %(table)s" % {'table': table}
                    sql_pd = common.select_sql(FXBYBY_DB, sqls)
                    pl = sql_pd.loc[0, 'pl']
                    if (pl == '1' and v > 0) or (pl == '-1' and v < 0):
                        result += k + str(v) + '_OK' + "\n"
                        break
            else:
                code1= code[:3]+ "/" + code[3:]
                bybypara = {'code': code1, 'amount': v, 'buysell': '買', 'kubun': '決済', 'nari_hiki': '0', 'settle': '', 'comment': '日時監視決済', 'now': '0'}
                if v < 0:
                    bybypara['amount'] = abs(v)
                    bybypara['buysell'] = '売'

                exec_time = f02_gmo.gmo_fx_main(bybypara)
                result += k + str(v) + '_NG' + str(exec_time) + "_" + bybypara['comment'] + "_" + bybypara['buysell'] + "\n"
                common.mail_send(u'FXポジションチェック', result)
                result = ""
        if len(dict_w) > 0:
            print(dict_w)
            common.insertDB3(FXBYBY_DB, "poji_fx", dict_w)


if __name__ == '__main__':  # 土曜日は5 datetime.datetime.now().weekday()
    info = F01_day_stgFX()
    today = datetime.date.today()
    argvs = sys.argv
    if argvs[1] == "Hours":

        t = datetime.datetime.now()
        i = int(t.strftime("%H"))
        info.main_fx()
        if i == 20 or i == 14:
            info.poji_check_main()

    if argvs[1] == "Weekly" and today.weekday() == 0:
        info.fx_weekly()
        info.weekly_exec()
        if info.send_msg != "":
            common.mail_send(u'FXトレード週間', info.send_msg)

    print("end", __file__)

#'EURJPY
#'20:55-21:00 売り
#'4:00-8:00 買い
#'USDJPY
#'9:55-15:00売り
#'EURUSD
#'8:00-10:00 売り
#'10:35-12:10 買い
#'16:55-21:00 売り
