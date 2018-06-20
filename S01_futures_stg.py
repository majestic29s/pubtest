import pandas as pd
import os
import datetime
import time
from pandas.io import sql
import sys
import glob

# 独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)
import s01_gmo

BYBY_DB = common.save_path('futures_byby.sqlite')
DB_INFO = common.save_path('futures.sqlite')


class S01_futures_stg(object):
    def __init__(self):
        self.row_arry = []
        self.send_msg = ""
        self.n225_anomari_amount = 3
        self.daily_exec_W_amount = 2
        self.n225_2130_exec_amount = 1
        self.weekly_exec_amount = 5
        self.Hikenari_amount = 1
        self.nigth_amount  = 1
        self.poji_flag_hike = common.save_path(common.DROP_DIR, "N225_BUY_HIKE")
        self.poji_flag_nigth = common.save_path(common.DROP_DIR, "N225_BUY_NIGTH")

    def round_up_down(self, val, opt, buysell):  # 10 -1
        val = int(val)
        ttt = round(val, opt)
        if val > ttt and buysell == '買':
            return ttt + 10
        if val < ttt and buysell == '売':
            return ttt - 10
        return ttt

    def n225_pojistion_count(self):
        dict_w = {}
        # アノマリーポジションチェック
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'poji'}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        dict_w['anomari'] = sql_pd['anomari'][0]
        # ポジションDB更新
        dict_w['buy'], dict_w['sell'] = s01_gmo.check_positon_mini()
        common.insertDB3(BYBY_DB, 'poji', dict_w)
        return int(dict_w['buy']), int(dict_w['sell'])

    def anomari_poji_update(self, amount):
        dict_w = {}
        table_name = 'poji'
        dict_w['anomari'] = amount
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])

    def anomari_poji_update2(self, dict_w):
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])

    def anomari_poji_select(self):
        table_name = 'ano_poji'
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        return sql_pd['amount'][0],sql_pd

    def N225_get_data(self):
        table_name = 'J225L'
        yest_day = str(datetime.date.today() - datetime.timedelta(days=7)).replace("-", "/") + ' 09:'
        dict_n = {}
        dict_d = {}
        dict_e = {}

        # 限月取得
        sqls = "select *,rowid from %(table)s where now > '%(key1)s'" % {'table': table_name, 'key1': yest_day}
        sql_pd = common.select_sql(DB_INFO, sqls)
        num = len(sql_pd)-2
        # 限月の妥当性チェック 使えるか?
        if sql_pd.loc[num, '現在値'] == '--':
            gen = sql_pd.loc[num+1, '限月']
        else:
            gen = sql_pd.loc[num, '限月']
        # 終値期限now取得
        dict = {'table': table_name, 'key2': yest_day, 'key3': gen}
        sqls = "select *,SUBSTR(now,12,2) as T,rowid from %(table)s where rowid=(select max(rowid) from %(table)s where 限月 = '%(key3)s') " % dict
        sql_pd = common.select_sql(DB_INFO, sqls)
        dict_e['now'] = sql_pd['now'][0]
        dict_e['始値'] = sql_pd['始値'][0]
        dict_e['高値'] = sql_pd['高値'][0]
        dict_e['安値'] = sql_pd['安値'][0]
        dict_e['終値'] = sql_pd['現在値'][0]
        dict_e['限月'] = gen
        #日中確定値
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'rashio'}
        sql_pd = common.select_sql('all_info.sqlite', sqls)
        dict_d['now'] = sql_pd['now'][0]
        dict_d['始値'] = sql_pd['N225openD'][0]
        dict_d['高値'] = sql_pd['N225highD'][0]
        dict_d['安値'] = sql_pd['N225lowD'][0]
        dict_d['終値'] = sql_pd['N225closeD'][0]
        #夜間確定値
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s where N225openN IS NOT NULL) ;" % {'table': 'rashio'}
        sql_pd = common.select_sql('all_info.sqlite', sqls)
        dict_n['now'] = sql_pd['now'][0]
        dict_n['始値'] = sql_pd['N225openN'][0]
        dict_n['高値'] = sql_pd['N225highN'][0]
        dict_n['安値'] = sql_pd['N225lowN'][0]
        dict_n['終値'] = sql_pd['N225closeN'][0]

        dict_e = common.to_number(dict_e)
        dict_d = common.to_number(dict_d)
        dict_n = common.to_number(dict_n)

        return dict_e, dict_d, dict_n

    def n225_weekly_update(self, upflag=1):  # upflag=1はVIとWEEK売りとの区別
        dict_w = {}
        DB_RASHIO = common.save_path('all_info.sqlite')
        yest_day = str(datetime.date.today() - datetime.timedelta(days=7)).replace("-", "/")
        dict = {'table': 'rashio', 'key1': yest_day}
        sqls = "select *,rowid from %(table)s where now > '%(key1)s'" % dict
        sql_pd = common.select_sql(DB_RASHIO, sqls)
        num = len(sql_pd)-1
        dict_w['now_last'] = sql_pd.loc[0, 'now']
        dict_w['始値'] = sql_pd.loc[0, 'N225openD']
        dict_w['高値'] = max(sql_pd['N225highD'])
        dict_w['安値'] = min(sql_pd['N225lowD'])
        dict_w['終値'] = sql_pd.loc[num, 'N225closeD']
        # データテーブルへ追加
        if upflag == 1:  # upflag=1はWEEK売りとの区別
            # rowid取得
            table_name = 'N225_Weekly'
            sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
            sql_pd = common.select_sql(BYBY_DB, sqls)
            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])
        return dict_w

    def daily_exec_D(self, table_name,tuple_w):  # n225_daily_updateと入れ替え
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        # N225データ取得
        dict_e, dict_d, dict_n = self.N225_get_data()
        if table_name == 'J225L_2130':
            dict_w['始値'] = dict_n['始値']
            dict_w['高値'] = dict_n['高値']
            dict_w['安値'] = dict_n['安値']
            dict_w['終値'] = dict_n['終値']
            dict_w['決済'] = dict_d['始値']
        else:
            dict_w['始値'] = dict_d['始値']
            dict_w['高値'] = dict_d['高値']
            dict_w['安値'] = dict_d['安値']
            dict_w['終値'] = dict_d['終値']
            dict_w['決済'] = dict_n['始値']

        # 前日仕掛けデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        buy = int(sql_pd['買いSTL'][0])
        sell = int(sql_pd['売りSTL'][0])
        if dict_w['高値'] >= buy:
            dict_w['買い決済'] = dict_w['終値'] - buy
            dict_w['買い合計'] = ""
        if dict_w['安値'] <= sell:
            dict_w['売り決済'] = sell - dict_w['終値']
            dict_w['売り合計'] = ""
        sqls = common.create_update_sql(
            BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        # ダウ取得
        DB_RASHIO = common.save_path('all_info.sqlite')
        dict = {'table': 'rashio'}
        sqls = "select INDU_IND,rowid from %(table)s where INDU_IND IS NOT NULL" % dict
        sql_pd = common.select_sql(DB_RASHIO, sqls)
        num = len(sql_pd)-1
        dict_w['DJI_DIFF'] = int(float(sql_pd.loc[num, 'INDU_IND']) - float(sql_pd.loc[num-1, 'INDU_IND']))
        # 仕掛値の切り上げ、切り下げ
        bf = 10
        dict_w['買いSTL'] = self.round_up_down(dict_e['高値']+bf, -1, "買")
        dict_w['売りSTL'] = self.round_up_down(dict_e['安値']-bf, -1, "売")
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ実行とデータ更新--------- #
        # ////////////////////////////////////////////////// #
        amount = self.n225_2130_exec_amount

        today = datetime.date.today()
        if today.weekday() in tuple_w:  # 金曜日以外
            bybypara = {'code': '銘柄選択', 'amount': amount, 'buysell': '売', 'kubun': '新規',
                        'nari_hiki': '', 'settle': dict_w['売りSTL'], 'comment': "N225"+'ナイトSTL売り'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "_" + str(dict_w['売りSTL']) + "_建玉:" + str(amount) + "\n"

            bybypara = {'code': '銘柄選択', 'amount': amount, 'buysell': '買', 'kubun': '新規',
                        'nari_hiki': '', 'settle': dict_w['買いSTL'], 'comment': "N225"+'ナイトSTL買い'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "_" + str(dict_w['買いSTL']) + "_建玉:" + str(amount) + "\n"
        dict_w.update(common.info_index())
        common.insertDB3(BYBY_DB, table_name, dict_w)
        # ポジションDB更新
        n_buy, n_sell = self.n225_pojistion_count()
        common.sum_clce(BYBY_DB, table_name, '買い決済', '買い合計', 0)
        common.sum_clce(BYBY_DB, table_name, '売り決済', '売り合計', 0)


    def n225_poji_check(self):
        # ////////////////////////////////////////////////// #
        # ---------------- ポジションDB更新チェック ---------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        # 前日仕掛けデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'poji'}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        l_buy = int(sql_pd['buy'][0])
        l_sell = int(sql_pd['sell'][0])
        # ポジションDB更新
        n_buy, n_sell = self.n225_pojistion_count()

        # ////////////////////////////////////////////////// #
        # ---------------- 決済処理 ---------------- #
        # ////////////////////////////////////////////////// #
        # 週次ポジション決済
        if common.week_start_day() == 1:
            files = common.save_path(common.DROP_DIR, "N225_*")
            anomari_amount = glob.glob(files)  # ワイルドカードが使用可能
            s_buy = n_buy
            s_sell = n_sell
            if len(anomari_amount) > 0:
                if anomari_amount[0].count("BUY"):
                    s_buy = n_buy - self.n225_anomari_amount
                    if s_buy < 0:
                        s_buy = 0
                elif anomari_amount[0].count("PUT"):
                    s_sell = n_sell - self.n225_anomari_amount
                    if s_sell < 0:
                        s_sell = 0

            if s_buy > 0:
                bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': s_buy, 'buysell': '買','kubun': '', 'nari_hiki': '', 'settle': 1, 'comment': 'N225買_週次_決済'}
                result, msg = s01_gmo.gmo_main(bybypara)
                self.send_msg += msg + "建玉:" + str(s_buy) + "\n"
            if s_sell > 0:
                bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': s_sell, 'buysell': '売','kubun': '', 'nari_hiki': '', 'settle': 1, 'comment': 'N225売_週次_決済'}
                result, msg = s01_gmo.gmo_main(bybypara)
                self.send_msg += msg + "建玉:" + str(s_sell) + "\n"

    def settle_hike(self):
        if os.path.exists(self.poji_flag_hike):
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': self.Hikenari_amount, 'buysell': '買', 'kubun': '', 'nari_hiki': '', 'settle': 1, 'comment': 'N225買_決済'}
            print(bybypara)
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "建玉:" + str(self.Hikenari_amount) + "\n"
            os.remove(self.poji_flag_hike)

        if os.path.exists(self.poji_flag_nigth):
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': self.nigth_amount, 'buysell': '買','kubun': '', 'nari_hiki': '', 'settle': 1, 'comment': 'N225買_決済'}
            print(bybypara)
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "建玉:" + str(self.nigth_amount) + "\n"
            os.remove(self.poji_flag_nigth)

    def weekly_exec(self, range_W='Today'):
        code = 'N225'
        table_name = code + "_Weekly"
        dict_w = {}
        last_C = 0
        last_C1 = 0

        sqls = "select *,rowid from " + table_name
        sql_pd = common.select_sql(BYBY_DB, sqls)

        # 全部計算する場合はToday以外を設定
        if range_W == 'Today':
            dfs = sql_pd[-3:]
        else:
            dfs = sql_pd
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
                    dict_w['買い決済'] = int(row['終値'] - dict_w['買いSTL'])
                    # 買い決済
                if row['安値'] < dict_w['売りSTL']:
                    dict_w['売り決済'] = int(dict_w['売りSTL'] - row['終値'])
                    # 売り決済
                # DBアップデート
                sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, row['rowid'])

            # 翌日のデータ作成
            dict_w = {}
            MAX_W = max(row['高値'], last_C)
            MIN_W = min(row['安値'], last_C)
            dict_w['変動幅'] = int(MAX_W - MIN_W)
            dict_w['幅85'] = int(dict_w['変動幅'] * 0.85)
            # 仕掛値の切り上げ、切り下げ
            dict_w['買いSTL'] = self.round_up_down(dict_w['幅85']+row['終値'], -1, "買")
            dict_w['売りSTL'] = self.round_up_down(row['終値']-dict_w['幅85'], -1, "売")

            last_C1 = last_C
            last_C = row['終値']
        if range_W == 'Today':
            dict_w['memo'] = ""
            dict_w['日付'] = common.env_time()[1][:10]
            # 買い売りしかけ
            numt = len(sql_pd)-1
            m=sql_pd['終値'].rolling(5).mean()
            dict_w['S0C_avg5'] = float(sql_pd.loc[numt, '終値']) / m[numt]
            dict_w['vora'] = dict_w['幅85'] / row['終値']
#            if dict_w['S0C_avg5'] < 1.05:
            if dict_w['vora'] < 0.03:
                bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': self.weekly_exec_amount, 'buysell': '売','kubun': '新規', 'nari_hiki': '', 'settle': dict_w['売りSTL'], 'comment': 'N225週次STL売り_週始'}
                result, msg = s01_gmo.gmo_main(bybypara)
                self.send_msg += msg + "\n"
                dict_w['memo'] = "sell"
#            if dict_w['S0C_avg5'] > 0.97:
            if dict_w['vora'] < 0.03:
                bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': self.weekly_exec_amount, 'buysell': '買','kubun': '新規', 'nari_hiki': '', 'settle': dict_w['買いSTL'], 'comment': 'N225週次STL買い_週始'}
                result,msg = s01_gmo.gmo_main(bybypara)
                self.send_msg += msg + "\n"
                dict_w['memo'] += "buy"
            common.insertDB3(BYBY_DB, table_name, dict_w)

        common.sum_clce(BYBY_DB, table_name, '買い決済', '買い合計', 0)
        common.sum_clce(BYBY_DB, table_name, '売り決済', '売り合計', 0)

    def n225_anomari(self):
        amount = self.n225_anomari_amount
        flag_buy = common.save_path(common.DROP_DIR, "N225_BUY" + "_" + str(amount))
        flag_sell = common.save_path(common.DROP_DIR, "N225_PUT" + "_" + str(amount))
        t = datetime.datetime.now()
        date_mmdd = t.strftime("%m%d")
        # 決済処理条件
        if ("0430" < date_mmdd < "0510" or "1231" < date_mmdd < "0110") and os.path.exists(flag_buy):
            print("N225アノマリー決済買")
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '買','kubun': '決済', 'nari_hiki': '', 'settle': 1, 'comment': 'N225アノマリー決済買'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "\n"
            os.remove(flag_buy)
            self.anomari_poji_update(0)
        if ("0614" < date_mmdd < "0620" or "0831" < date_mmdd < "0910") and os.path.exists(flag_sell):
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '売','kubun': '決済', 'nari_hiki': '', 'settle': 1, 'comment': 'N225アノマリー決済売'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "\n"
            os.remove(flag_sell)
            self.anomari_poji_update(0)

        # フラグがある場合は終了
        if os.path.exists(flag_buy) or os.path.exists(flag_sell):
            return
        # 仕掛処理条件
        if "0315" < date_mmdd < "0401" or "1115" < date_mmdd < "1201":
            common.create_file(flag_buy)
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '買','kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': 'N225アノマリー新規買'}
            if "1115" < date_mmdd < "1201" or "0101" < date_mmdd < "0110":
                bybypara['nari_hiki'] = '次限月'
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "\n"
            self.anomari_poji_update(amount)
        if "0501" < date_mmdd < "0515" or "0715" < date_mmdd < "0801":
            print("N225アノマリー新規売")
            common.create_file(flag_sell)
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '売','kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': 'N225アノマリー新規売'}
            if "0501" < date_mmdd < "0515":
                bybypara['nari_hiki'] = '次限月'
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "\n"
            self.anomari_poji_update(amount*-1)

    def IV_weekly_End(self):
        table_name = "N225_Weekly_IV"
        dict_w = self.n225_weekly_update("-")

        # 週データ実績
        dict_w2 = {}
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        t_stop = int(sql_pd['stop'][0])
        t_sum = int(sql_pd['sum'][0])
        dict_w2["決済_始値"] = dict_w["始値"]
        dict_w2["決済_終値"] = dict_w["終値"]
        if t_stop == 0 and t_sum > 0:
            dict_w2["決済"] = int(dict_w["終値"]) - int(dict_w['始値'])
        elif t_stop == 0 and t_sum < 0:
            dict_w2["決済"] = int(dict_w["始値"]) - int(dict_w['終値'])
        # DB更新
        sqls = common.create_update_sql(BYBY_DB, dict_w2, table_name, sql_pd['rowid'][0])

        # クリック365
        UURL = "http://tfx.jfx.jiji.com/cfd/quote"
        dfs = pd.read_html(UURL, header=0)
        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            for ii in range(len(row)):
                if dfs[0].columns[ii] == "直近約定値":
                    sp_work = row[ii].split("(")
                    dict_w["C_365"] = int(sp_work[0].replace(",", ""))
            break

        UURL = "http://quote.jpx.co.jp/jpx/template/quote.cgi?F=tmp/real_index2&QCODE=145"
        dfs = pd.read_html(UURL, header=0)
        for idx, row in dfs[0].iterrows():
            for ii in range(len(row)):
                if dfs[0].columns[ii] == "終値":
                    sp_work = row[ii].split("(")
                    dict_w["IV"] = sp_work[0].replace(",", "")
            break
        # 週始のデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        print(sql_pd['SO_365'][0])
        dict_w["O_365"] = sql_pd['SO_365'][0]
        dict_w["週間投資戦略日経平均"] = round(
            (int(dict_w["終値"]) / int(dict_w['始値'])) - 1, 2)
        dict_w["週間投資戦略365"] = round(
            (int(dict_w["C_365"]) / int(dict_w['O_365'])) - 1, 2)
        dict_w["日経平均ＶＩ週間変化"] = round(
            (float(dict_w["IV"]) - float(sql_pd['IV'][0])), 2)
        dict_w["週間リターン日経平均"] = round(
            (int(dict_w["終値"]) / int(sql_pd['終値'][0])) - 1, 2)
        dict_w["週間リターン365"] = round(
            (int(dict_w["C_365"]) / int(sql_pd['C_365'][0])) - 1, 2)
        if dict_w["週間リターン日経平均"] > 0 and dict_w["日経平均ＶＩ週間変化"] > 0:
            dict_w["type"] = 11
        if dict_w["週間リターン日経平均"] > 0 and dict_w["日経平均ＶＩ週間変化"] < 0:
            dict_w["type"] = 10
        if dict_w["週間リターン日経平均"] < 0 and dict_w["日経平均ＶＩ週間変化"] > 0:
            dict_w["type"] = 1
        if dict_w["週間リターン日経平均"] < 0 and dict_w["日経平均ＶＩ週間変化"] < 0:
            dict_w["type"] = 0
        common.insertDB3(BYBY_DB, table_name, dict_w)


    def IV_Create_Flag(self):
        table_name = "N225_Weekly_IV"
        # 週始のデータ取得

        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        week_n225 = float(sql_pd['週間リターン日経平均'][0])
        week_vi = float(sql_pd['日経平均ＶＩ週間変化'][0])
        vi = float(sql_pd['IV'][0])
#        n225_s = float(sql_pd['寄り付き乖離率日経平均'][0])
        n225_s = float(sql_pd['週間リターン日経平均'][0])
        week_365 = float(sql_pd['週間リターン365'][0])
        type = int(sql_pd['type'][0])

        if type == 0:
            dict_w = self.flag0(week_n225, week_vi, vi, n225_s)
        if type == 1:
            dict_w = self.flag1(week_n225, week_vi, vi, n225_s)
        if type == 10:
            dict_w = self.flag10(week_n225, week_vi, vi, n225_s)
        if type == 11:
            dict_w = self.flag11(week_n225, week_vi, vi, n225_s, week_365)
        # データ集計
        dict_w['sum'] = sum(dict_w.values())
        dict_w['stop'] = len([i for i in dict_w.values() if i == 0])
        # DB更新
        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])

        #トレード
        if dict_w['stop'] > 0:
            return 0
        amount = 2
        if dict_w['sum'] > 0:
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '買','kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': 'N225VI週次新規買'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "\n"
        if dict_w['sum'] < 0:
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '売','kubun': '新規', 'nari_hiki': '', 'settle': 0, 'comment': 'N225VI週次新規売'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "\n"


    def flag11(self, week_n225, week_vi, vi, n225_s, week_365):
        dict_w = {}
        # フラグ１1
        if week_365 < 0:
            dict_w['flag1'] = 0
        if vi < 17.2:
            dict_w['flag2'] = 0
        if vi > 17.2 and n225_s > 0.015:
            dict_w['flag3'] = 1
        if 17.2 < vi < 23.3 and week_vi < 0.5 and n225_s < 0.009:
            dict_w['flag4'] = 1
        if 17.2 < vi < 21.9 and week_vi > 0.5 and n225_s < 0.015:
            dict_w['flag5'] = -1
        if 21.9 < vi < 23.3 and week_vi > 0.5 and n225_s < 0.015:
            dict_w['flag6'] = 0
        if 23.3 < vi and week_n225 < 0.043 and n225_s < 0.015:
            dict_w['flag7'] = -1
        if 23.3 < vi and week_n225 > 0.058 and n225_s < 0.015:
            dict_w['flag8'] = 1
        return dict_w

    def flag10(self, week_n225, week_vi, vi, n225_s):
        dict_w = {}
        # フラグ１0
        if vi < 20 and week_n225 < 0.009:
            dict_w['flag1'] = 1
        if vi > 20 and -0.2 < week_vi < 0:
            dict_w['flag2'] = 0
        if vi < 29 and (vi > 20 or week_n225 > 0.009) and 0 < week_vi < 0.003:
            dict_w['flag4'] = 0
        if vi < 29 and (vi > 20 or week_n225 > 0.009) and 0 < n225_s and week_n225 < 0.04:
            dict_w['flag5'] = 0
        if vi < 29 and (vi > 20 or week_n225 > 0.009) and 0 < n225_s < 0.04 and week_vi < -0.5:
            dict_w['flag6'] = 1
        if vi < 29 and (vi > 20 or week_n225 > 0.009) and -0.4 < week_vi < -0.02 and n225_s > 0 and week_n225 < 0.04:
            dict_w['flag7'] = -1
        if vi > 34 and week_vi < -0.4:
            dict_w['flag8'] = 1
        return dict_w

    def flag1(self, week_n225, week_vi, vi, n225_s):
        dict_w = {}
        # フラグ１
        if vi < 24 and n225_s < -0.07:
            dict_w['flag1'] = -1
        if vi < 24 and week_vi < 1.7 and -0.007 < n225_s < 0.012 and week_n225 < -0.02:
            dict_w['flag2'] = 1
        if vi < 17 and week_vi < 1.7 and -0.007 < n225_s < 0.012 and week_n225 > 0.02:
            dict_w['flag3'] = 1
        if 17 < vi < 24 and week_vi < 1.7 and -0.007 < n225_s < 0.008 and week_n225 > -0.02:
            dict_w['flag4'] = -1
        if 17 < vi < 24 and week_vi < 1.7 and 0.008 < n225_s < 0.012 and week_n225 > -0.02:
            dict_w['flag5'] = 1
        if vi < 24 and 1.7 < week_vi < 2.1 and -0.007 < n225_s < 0.012:
            dict_w['flag6'] = 1
        if vi < 24 and 2.1 < week_vi < 2.7 and -0.007 < n225_s < 0.012 and week_n225 < -0.017:
            dict_w['flag7'] = 1
        if vi < 24 and 2.7 < week_vi < 3.1 and -0.007 < n225_s < 0.012 and week_n225 < -0.017:
            dict_w['flag8'] = -1
        if vi < 24 and 2.1 < week_vi and -0.007 < n225_s < 0.012 and week_n225 > -0.017:
            dict_w['flag9'] = 1
        if vi < 24 and 0.012 < week_vi:
            dict_w['flag10'] = 1
        if 24 < vi < 29 and 4 < week_vi:
            dict_w['flag11'] = -1
        if 24 < vi < 29 and 4 > week_vi and n225_s < -0.008:
            dict_w['flag12'] = 1
        if 24 < vi < 29 and 4 > week_vi and n225_s > -0.008 and week_n225 < -0.036:
            dict_w['flag13'] = 1
        if 24 < vi < 29 and 4 > week_vi and week_n225 > -0.02:
            dict_w['flag14'] = 1
        if 29 < vi and week_vi > 0.024 and n225_s < -0.05:
            dict_w['flag15'] = -1
        if 29 < vi and week_vi > 1.2 and n225_s < -0.005:
            dict_w['flag16'] = 1
        return dict_w

    def flag0(self, week_n225, week_vi, vi, n225_s):
        dict_w = {}
        # フラグ0
        if week_n225 < -0.032 or week_vi < -4:
            dict_w['flag1'] = 0
        if vi < 20 and week_n225 < -0.015:
            dict_w['flag2'] = 1
        if vi < 20 and week_n225 > -0.015:
            dict_w['flag3'] = -1
        if 20 < vi < 29 and n225_s < 0 and week_vi < -1.3:
            dict_w['flag5'] = -1
        if 20 < vi < 29 and n225_s < 0 and week_vi > -1.3:
            dict_w['flag6'] = 0
        if 20 < vi < 29 and n225_s > 0:
            dict_w['flag7'] = 1
        if vi > 29:
            dict_w['flag8'] = 1
        return dict_w

    def IV_weekly_Start(self):  # 840
        table_name = "N225_Weekly_IV"
        dict_w = {}
        # クリック365
        UURL = "http://tfx.jfx.jiji.com/cfd/quote"
        dfs = pd.read_html(UURL, header=0)
        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            for ii in range(len(row)):
                if dfs[0].columns[ii] == "始値":
                    dict_w["SO_365"] = int(row[ii])
            break
        UURL = "http://quote.jpx.co.jp/jpx/template/quote.cgi?F=tmp/real_index2&QCODE=145"
        dfs = pd.read_html(UURL, header=0)
        for idx, row in dfs[0].iterrows():
            for ii in range(len(row)):
                if dfs[0].columns[ii] == "終値":
                    sp_work = row[ii].split("(")
                    dict_w["O_IV"] = sp_work[0].replace(",", "")
        # 売り買い気配取得
        dict_t = s01_gmo.check_new_data()
        dict_w["気配値"] = dict_t['N225real']

        # DBUPDATE
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)

        # 計算
        dict_w["寄り付き乖離率日経平均"] = round((int(dict_w["気配値"]) / int(sql_pd['終値'][0])) - 1, 2)
        dict_w["寄り付き乖離率365"] = round((int(dict_w["SO_365"]) / int(sql_pd['C_365'][0])) - 1, 2)
        # DB更新
        sqls = common.create_update_sql(
            BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])

    def daily_exec_W(self):
        # 本日のリアルデータ
        dict_t = s01_gmo.check_new_data()
        # 週次データ
        code = 'N225'
        table_name = code + "_Weekly"
        sqls = "select *,rowid from " + table_name
        sql_pd = common.select_sql(BYBY_DB, sqls)
        num = len(sql_pd) - 1
        t_min = min(dict_t['N225lowD'], dict_t['N225lowN'])
        t_max = max(dict_t['N225highD'], dict_t['N225highN'])

#        if float(sql_pd.loc[num, '幅85']) / int(sql_pd.loc[num-1, '終値']) < 0.03 and t_min > sql_pd.loc[num, '売りSTL']:
        if sql_pd.loc[num, 'memo'].count('buy') and t_min > sql_pd.loc[num, '売りSTL']:
            print('N225週次STL売り', sql_pd.loc[num, '売りSTL'])
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': self.daily_exec_W_amount, 'buysell': '売', 'kubun': '新規','nari_hiki': '', 'settle': sql_pd.loc[num, '売りSTL'], 'comment': 'N225週次STL売り'}
            result, msg = s01_gmo.gmo_main(bybypara)

#        if  float(sql_pd.loc[num, '幅85']) / int(sql_pd.loc[num-1, '終値']) < 0.03  and t_max < sql_pd.loc[num, '買いSTL']:
        if sql_pd.loc[num, 'memo'].count('sell') and t_max < sql_pd.loc[num, '買いSTL']:
            print("N225週次STL買い", sql_pd.loc[num, '買いSTL'])
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': self.daily_exec_W_amount, 'buysell': '買', 'kubun': '新規','nari_hiki': '', 'settle': sql_pd.loc[num,'買いSTL'], 'comment': 'N225週次STL買い'}

            result,msg = s01_gmo.gmo_main(bybypara)

    def hikenari_1450(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        table_name = 'J225L_1450'
        # N225データ取得
        dict_e, dict_d, dict_n = self.N225_get_data()
        dict_w['決済1630'] = dict_n['始値']
        # 前日仕掛けデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) and 損益 IS NULL;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        if len(sql_pd) > 0:
            dict_w['損益'] = dict_w['決済1630'] - sql_pd['現在1450'][0]
            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = self.N225_get_1450()
        # 本日のリアルデータ
#        dict_t = s01_gmo.check_new_data()
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ実行とデータ更新--------- #
        # ////////////////////////////////////////////////// #
        if dict_w['現在0930'] > dict_w['現在1400'] > dict_w['現在1450']:
            common.create_file(self.poji_flag_hike)
            amount = self.Hikenari_amount
            dict_w['memo'] = "buy"
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '買', 'kubun': '新規','nari_hiki': '', 'settle': 0, 'comment': '成り行き買い1450'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg  + "_建玉:" + str(amount) + "\n"
        else:
            dict_w['損益'] = 0
        dict_w.update(common.info_index())
        common.insertDB3(BYBY_DB, table_name, dict_w)
        # ポジションDB更新
#        n_buy, n_sell = self.n225_pojistion_count()
#        common.sum_clce(BYBY_DB, table_name, '決済', '合計', 0)


    def saya_1500(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        dict_wL = {}
        table_name = 'saya_1515'
        # 前日のデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s);" % {'table': table_name}
        sql_pd3 = common.select_sql(BYBY_DB, sqls)

        # N225データ取得
        sqls = "select *,rowid from %(table)s ;" % {'table': "J225L"}
        sql_pd = common.select_sql(DB_INFO, sqls)
        cnt = len(sql_pd) -2
        dict_w['Nnow'] = sql_pd['now'][cnt]
        dict_w['N始値'] = sql_pd['始値'][cnt]
        dict_w['N高値'] = sql_pd['高値'][cnt]
        dict_w['N安値'] = sql_pd['安値'][cnt]
        dict_w['N現在値'] = sql_pd['現在値'][cnt]
        dict_w['N前日乖離'] = round(int(sql_pd['現在値'][cnt]) / int(sql_pd['前日終値'][cnt]), 3)
        dict_w['N前日差額'] = int(sql_pd['現在値'][cnt]) - int(sql_pd['前日終値'][cnt])
        dict_wL['N終値'] = sql_pd['前日終値'][cnt]

        # TOPIXデータ取得
        sqls = "select *,rowid from %(table)s ;" % {'table': "topixL"}
        sql_pd = common.select_sql(DB_INFO, sqls)
        cnt = len(sql_pd) -2
        dict_w['T始値'] = sql_pd['始値'][cnt]
        dict_w['T高値'] = sql_pd['高値'][cnt]
        dict_w['T安値'] = sql_pd['安値'][cnt]
        dict_w['T現在値'] = sql_pd['現在値'][cnt]
        dict_w['T前日乖離'] = round(float(sql_pd['現在値'][cnt]) / float(sql_pd['前日終値'][cnt]),3)
        dict_w['T前日差額'] = float(sql_pd['現在値'][cnt]) - float(sql_pd['前日終値'][cnt])
        dict_wL['T終値'] = sql_pd['前日終値'][cnt]

        sqls = "select *,rowid from %(table)s;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        sql_pd = common.to_number(sql_pd)
        cnt = len(sql_pd) - 1
        para = 25
        aaa = sql_pd['N現在値'].rolling(para).mean()
        dict_w['Navg25'] = round(float(dict_w['N現在値']) / float(aaa[cnt]),5)
        aaa = sql_pd['T現在値'].rolling(para).mean()
        dict_w['Tavg25'] = round(float(dict_w['T現在値']) / float(aaa[cnt]), 5)

        dict_w['compNT'] = int(round(float(dict_w['N現在値']) / float(dict_w['T現在値']), -1))
        dict_w['avg_NT'] = round(dict_w['Navg25'] / dict_w['Tavg25'],5)
        dict_w['前日差額'] = dict_w['N前日差額'] - (dict_w['T前日差額'] * dict_w['compNT'])

        if dict_w['avg_NT'] > 1 and dict_w['前日差額'] > 30:
            dict_w['仕掛け'] = 1
            if int(sql_pd3['仕掛け'][0]) == -1:#決済
                pass
                #ここに鞘の決済追加 TOPIX 買い 225売り
            if int(sql_pd3['仕掛け'][0]) != 1:#新規仕掛け
                #ここに鞘の仕掛け追加 TOPIX 売り 225買い
                pass
        elif dict_w['avg_NT'] < 1 and dict_w['前日差額'] < 50: #-30から変更
            dict_w['仕掛け'] = -1
            if int(sql_pd3['仕掛け'][0]) == 1:#決済
                #ここに鞘の決済追加 TOPIX 売り 225買い
                pass
            if int(sql_pd3['仕掛け'][0]) != -1:#新規仕掛け
                #ここに鞘の仕掛け追加 TOPIX 買い 225売り
                pass
        else:
            dict_w['仕掛け'] = 0

        # N225乖離-0.03前日比-100
        if dict_w['Navg25'] < 0.97 and dict_w['N前日差額'] < -100:
            dict_w['仕掛け乖離'] = 1
            bybypara = {'code': '銘柄選択', 'amount': 1, 'buysell': '買', 'kubun': '新規','nari_hiki': '', 'settle': 0, 'comment': '引成買い1513'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg  + "_建玉:" + str(bybypara['amount']) + "\n"

        # JPX400データ取得
        sqls = "select *,rowid from %(table)s ;" % {'table': "jpx400"}
        sql_pd = common.select_sql(DB_INFO, sqls)
        cnt = len(sql_pd) - 2
        dict_w['JPX始値'] = sql_pd['始値'][cnt]
        dict_w['JPX高値'] = sql_pd['高値'][cnt]
        dict_w['JPX安値'] = sql_pd['安値'][cnt]
        dict_w['JPX現在値'] = sql_pd['現在値'][cnt]
        dict_w['JPX前日乖離'] = round(int(sql_pd['現在値'][cnt]) / int(sql_pd['前日終値'][cnt]), 3)
        dict_wL['JPX終値'] = sql_pd['前日終値'][cnt]
        # マザーズデータ取得
        sqls = "select *,rowid from %(table)s ;" % {'table': "mather"}
        sql_pd = common.select_sql(DB_INFO, sqls)
        cnt = len(sql_pd) - 2
        dict_w['M始値'] = sql_pd['始値'][cnt]
        dict_w['M高値'] = sql_pd['高値'][cnt]
        dict_w['M安値'] = sql_pd['安値'][cnt]
        dict_w['M現在値'] = sql_pd['現在値'][cnt]
        dict_w['M前日乖離'] = round(float(sql_pd['現在値'][cnt]) / float(sql_pd['前日終値'][cnt]),3)
        dict_wL['M終値'] = sql_pd['前日終値'][cnt]
        if dict_w['M前日乖離'] < 1  :
            dict_w['M仕掛け'] = 1
        else:
            dict_w['M仕掛け'] = 0
        dict_w.update(common.info_index())
        common.insertDB3(BYBY_DB, table_name, dict_w)
        dict_w = common.to_number(dict_w)
        dict_wL = common.to_number(dict_wL)
        sql_pd3 = common.to_number(sql_pd3)
        # DB更新
        if len(sql_pd3) > 0:
            if sql_pd3['仕掛け'][0] == 1:
                dict_wL['決済'] = dict_w['前日差額']
            if sql_pd3['仕掛け'][0] == -1:
                dict_wL['決済'] = dict_w['前日差額'] * -1
            if sql_pd3['M仕掛け'][0] == 1:
                dict_wL['M決済'] = dict_w['M始値'] - dict_wL['M終値']
            if sql_pd3['仕掛け乖離'][0] == 1:
                dict_e, dict_d, dict_n = self.N225_get_data()
                dict_wL['乖離決済'] = dict_n['始値'] - dict_wL['N終値']

            sqls = common.create_update_sql(BYBY_DB, dict_wL, table_name, sql_pd3['rowid'][0])



    def nigth_settle(self,title,code='銘柄選択'):
        dict_w = {}
        dict_w['buy'], dict_w['sell'] = s01_gmo.check_positon_large()
        print(dict_w['buy'], dict_w['sell'])
        if code == '銘柄選択(ﾐﾆ)':
            dict_w['buy'] = 1
        if dict_w['buy'] > 0:
            bybypara = {'code': code, 'amount': dict_w['buy'], 'buysell': '買','kubun': '', 'nari_hiki': '', 'settle': 1, 'comment': title + '買い_決済'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "建玉:" + str(self.Hikenari_amount) + "\n"
        if dict_w['sell'] > 0:
            bybypara = {'code': code, 'amount': dict_w['sell'], 'buysell': '売','kubun': '', 'nari_hiki': '', 'settle': 1, 'comment': title + '売り_決済'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg + "建玉:" + str(self.Hikenari_amount) + "\n"

    def N225_get_1450(self, Stime=' 09:30'):
        code = 'N225L'
        table_name = 'J225L'
        yest_day = str(datetime.date.today() - datetime.timedelta(days=0)).replace("-", "/") + Stime
        dict_e = {}

        # 限月取得
        dict = {'table': table_name, 'key1': yest_day, 'key2': ''}
        sqls = "select *,rowid from %(table)s where now > '%(key1)s'" % dict
        sql_pd = common.select_sql(DB_INFO, sqls)
        num = len(sql_pd)-2
        gen = sql_pd.loc[num, '限月']
        # 限月の妥当性チェック 使えるか?
        if sql_pd.loc[num, '現在値'] == '--':
            gen = sql_pd.loc[num-1, '限月']
        # 終値期限now取得
        dict = {'table': table_name, 'key2': yest_day, 'key3': gen}
        sqls = "select *,SUBSTR(now,12,2) as T,rowid from %(table)s where 限月 = '%(key3)s' and  now > '%(key2)s'" % dict
        sql_pd = common.select_sql(DB_INFO, sqls)
        num = len(sql_pd)-1
        dict_e['now0930'] = sql_pd.loc[0, 'now']
        dict_e['現在0930'] = sql_pd.loc[0, '現在値']
        dict_e['now1400'] = sql_pd.loc[num-3, 'now']
        dict_e['現在1400'] = sql_pd.loc[num-3, '現在値']
        dict_e['now1450'] = sql_pd.loc[num, 'now']
        dict_e['現在1450'] = sql_pd.loc[num, '現在値']
        dict_e['前日終値'] = sql_pd.loc[num, '前日終値']
        dict_e = common.to_number(dict_e)
        return dict_e

    def hikenari_0520(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        table_name = 'J225L_0520'
        # N225データ取得
        dict_e, dict_d, dict_n = self.N225_get_data()
        dict_w['決済'] = dict_d['始値'] #s3_end
        dict_w['仕掛'] = dict_n['終値'] #s2_str
        # 前日仕掛けデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) and 損益 IS NULL;" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        if len(sql_pd) > 0:
            dict_w['損益'] = dict_w['決済'] - dict_w['仕掛']
            sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        dict_w['始値'] = dict_e['始値'] #s1_O
        dict_w['高値'] = dict_e['高値'] #s1_H
        dict_w['安値'] = dict_e['安値'] #s1_L
        dict_w['終値'] = dict_e['終値'] #s1_C
        dict_w['変動率'] = round(dict_e['終値'] / dict_n['終値'],5) #vora_R s0_O

        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ実行とデータ更新--------- #
        # ////////////////////////////////////////////////// #
        if dict_w['変動率'] < 1.01 and datetime.date.today().weekday() != 4:
            common.create_file(self.poji_flag_hike)
            amount = self.Hikenari_amount
            dict_w['memo'] = "buy"
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': amount, 'buysell': '買', 'kubun': '新規','nari_hiki': '', 'settle': 0, 'comment': 'N225引成買い'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg  + "_建玉:" + str(amount) + "\n"
        else:
            dict_w['損益'] = 0
        dict_w.update(common.info_index())
        common.insertDB3(BYBY_DB, table_name, dict_w)
        # ポジションDB更新
#        n_buy, n_sell = self.n225_pojistion_count()
#        common.sum_clce(BYBY_DB, table_name, '決済', '合計', 0)

    def over_night(self):
        # ////////////////////////////////////////////////// #
        # ---------------- 前日データ更新 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w = {}
        table_name = 'J225_over_night'
        # N225データ取得
        dict_e, dict_d, dict_n = self.N225_get_data()
        dict_w['S3_O'] = dict_e['始値']
        dict_w['S2_O'] = dict_n['始値']
        # 前日仕掛けデータ取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s);" % {'table': table_name}
        sql_pd = common.select_sql(BYBY_DB, sqls)
        if sql_pd['memo'][0] == "buy":
            dict_w['LongPL'] = dict_w['S3_O'] - dict_w['S2_O']
        sqls = common.create_update_sql(BYBY_DB, dict_w, table_name, sql_pd['rowid'][0])
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ取得 ---------------- #
        # ////////////////////////////////////////////////// #
        dict_w['S0_C'] = dict_n['終値']
        dict_w['S1_C'] = dict_e['終値']

        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s);" % {'table': 'J225L_1450'}
        sql_pd2 = common.select_sql(BYBY_DB, sqls)
        dict_w['S1_14'] = int(sql_pd2['現在1400'][0])
        # ////////////////////////////////////////////////// #
        # ---------------- 仕掛データ実行とデータ更新--------- #
        # ////////////////////////////////////////////////// #

        if dict_w['S1_14'] > dict_w['S1_C'] and dict_w['S0_C'] < dict_w['S1_C'] and common.weeks() != 'Tue':
            common.create_file(self.poji_flag_nigth)
            dict_w['amount'] = self.nigth_amount
            dict_w['memo'] = "buy"
            bybypara = {'code': '銘柄選択(ﾐﾆ)', 'amount': dict_w['amount'], 'buysell': '買', 'kubun': '新規','nari_hiki': '', 'settle': 0, 'comment': 'オーバーナイト買い'}
            result, msg = s01_gmo.gmo_main(bybypara)
            self.send_msg += msg  + "_建玉:" + str(dict_w['amount']) + "\n"
        dict_w.update(common.info_index())
        common.insertDB3(BYBY_DB, table_name, dict_w)
        # ポジションDB更新
#        n_buy, n_sell = self.n225_pojistion_count()
#        common.sum_clce(BYBY_DB, table_name, '決済', '合計', 0)

if __name__ == '__main__':  # 土曜日は5 datetime.datetime.now().weekday()
    info = S01_futures_stg()
    argvs = sys.argv
    today = datetime.date.today()
    if argvs[1] == "nigth_settle":  # 525位
        info.nigth_settle('N225引成')
        info.hikenari_0520()

    if argvs[1] == "daily_exit":  # 800位
        info.nigth_settle('N225成行')
        info.n225_anomari()
        info.n225_poji_check()
        info.settle_hike()

        if common.week_start_day() == 1:  # 800位
            info.n225_weekly_update()
            info.weekly_exec()

    if argvs[1] == "IV_weekly_Start" and common.week_start_day() == 1:
        info.IV_weekly_Start()
        info.IV_Create_Flag()

    if argvs[1] == "IV_weekly_End":#週末実行
        info.IV_weekly_End()

    if argvs[1] == "daily_exec":  # 930位
        info.daily_exec_D('J225L_0930', [1])

    if argvs[1] == "weekly_exec_day":  # 1631分 846分
        info.daily_exec_W()

    if argvs[1] == "hikenari_1450":  # 1450位
        info.nigth_settle('N225引成')
        info.hikenari_1450()

    if argvs[1] == "hikenari_1500":  # 1510位
        info.saya_1500()

    if argvs[1] == "hikenari_settle":  # 1610位
        info.nigth_settle('N225成行')
        info.settle_hike()
        info.over_night() #settle_hikeの前に実施

    if argvs[1] == "nigth_exec":  # 2100位
        info.daily_exec_D('J225L_2130', [0,1,2,3])

    common.mail_send(u'N225トレード_Daily', info.send_msg)


    print("end", __file__)
