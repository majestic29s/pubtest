
import sqlite3
import pandas.io.sql as psql
import pandas as pd
import urllib.request
import sys
import xlrd
import datetime
import time
import csv
import os
import requests
from bs4 import BeautifulSoup
import shutil
import filecmp
import numpy as np
import glob
# 独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)

DB_BYBY = common.save_path('B01_stock.sqlite')


class k51_DBupdate(object):
    def __init__(self):
        self.checked = "TEST"
        self.row_arry = []
        self.send_msg = ""
        self.flag_dir = common.TDNET_FLAG

    def download_file(self, url, filename):
        print('Downloading ... {0} as {1}'.format(url, filename))
        urllib.request.urlretrieve(url, filename)

    def main(self):
        # 貸借区分削除
        sqls = "UPDATE %(table)s SET 貸借区分 = '%(key2)s', 代用区分 = '%(key3)s',上場区分 = '%(key4)s'" % {'table': 'kabu_list', 'key1': "",'key2': "", 'key3': "", 'key4': ""}
        common.sql_exec(DB_BYBY, sqls)

        # 貸借区分ファイル
        dir_list = ["99_dict", "D_K_02_other"]
        file_path = common.save_path(common.dir_join(dir_list), "other.xlsx")

        book = xlrd.open_workbook(file_path)
        sheet = book.sheet_by_index(0)

        # 貸借区分DBアップデート
        for row_index in range(6, sheet.nrows):
            code = str(sheet.cell_value(rowx=row_index, colx=0)).replace(".0", "")
            val1 = str(sheet.cell_value(rowx=row_index, colx=2)).replace(".0", "")
            val2 = str(sheet.cell_value(rowx=row_index, colx=3)).replace(".0", "")
            val3 = str(sheet.cell_value(rowx=row_index, colx=4)).replace(".0", "")

            dict = {'table': 'kabu_list', 'key1': code, 'key2': val1,'key3': val2, 'key4': val3, 'key5': common.env_time()[1]}
            sqls = "select *,rowid from %(table)s where コード = '%(key1)s'" % dict
            tsd = common.select_sql(DB_BYBY, sqls)
            if len(tsd) > 0:
                sqls = "UPDATE %(table)s SET 貸借区分 = '%(key2)s', 代用区分 = '%(key3)s',上場区分 = '%(key4)s','uptime' = '%(key5)s' where コード = '%(key1)s'" % dict
                common.sql_exec(DB_BYBY, sqls)
            else:
                # 存在しない場合、追加
                val0 = str(sheet.cell_value(rowx=row_index, colx=1)).replace(".0", "")
                db_dict = {'コード': code, '銘柄名': val0, '貸借区分': val1,'代用区分': val2, '上場区分': val3}
                common.insertDB3(DB_BYBY, 'kabu_list', db_dict)
                self.send_msg += val0 + "を追加しました。" + "\n"

        # 削除ファイルリスト化
        dict = {'table': 'kabu_list', 'key1': "",'key2': "", 'key3': "", 'key4': ""}
        sqls = "select *,rowid from %(table)s where 貸借区分 = '%(key2)s'" % dict
        tsd = common.select_sql(DB_BYBY, sqls)
        if len(tsd) > 0:
            self.send_msg += str(len(tsd)) + "_個削除しました。"
        # 上場廃止銘柄確認・削除
        sqls = "delete from %(table)s where 貸借区分 = '%(key2)s'" % dict
        common.sql_exec(DB_BYBY, sqls)

        # 削除ファイル重複行
#        conn = sqlite3.connect(DB_BYBY)
#        sqls = "SELECT rowid ,コード ,銘柄名 FROM kabu_list GROUP BY コード HAVING COUNT(コード) > 1"
#        tsd = pd.read_sql(sqls,conn)
#        conn.close()
#        if len(tsd) > 0 :
#            for i,row in tsd.iterrows():
#                common.to_number(row)
#                dict = {'table':'kabu_list','key1':row['コード'],'key2':"",'key3':"",'key4':""}
#                sqls = "delete from %(table)s where コード = '%(key1)s'" % dict
#                common.sql_exec(DB_BYBY,sqls)
#                self.send_msg += str(row['rowid']) + row['銘柄名'] + "の重複を削除しました。"  + "\n"

    def main2(self):
        url = "http://www.taisyaku.jp/sys-list/data/seigenichiran.xls"
        dir_list = ["99_dict", "D_K_03_data"]
        file_path = common.save_path(common.dir_join(dir_list), "seigenichiran.xls")
        book = xlrd.open_workbook(file_path)
        sheet = book.sheet_by_index(0)

        # 貸借区分DBアップデート
        for row_index in range(5, sheet.nrows):
            code = str(sheet.cell_value(
                rowx=row_index, colx=1)).replace(".0", "")
            stuts = sheet.cell_value(rowx=row_index, colx=3)
            sellstop = sheet.cell_value(rowx=row_index, colx=6)
            if stuts == "申込停止" and sellstop != "":
                sqls = "UPDATE %(table)s SET 貸借区分 = '%(key2)s', uptime = '%(key3)s' where コード = '%(key1)s'" % {'table': 'kabu_list', 'key1': code,'key2': stuts, 'key3': common.env_time()[1]}
                common.sql_exec(DB_BYBY, sqls)

    def ment3(self):
        sqls = "select *,rowid from kabu_list"
        sql_pd = common.select_sql(DB_BYBY, sqls)
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            code = row['コード']
            yahoo = common.real_stock2(code)
            if yahoo['LastDay'] == 0:
                continue
            # 売りイベントチェック
            if common.date_diff(row['SELL_EVENT']) < -80:
                row['SELL_EVENT'] = ''
            if row['セクタ'] == 'REIT銘柄一覧' or row['セクタ'] == 'REIT':
                yahoo['amount'] = row['単位']
            dict_w = {'単位': yahoo['amount'], '信用倍率': yahoo['trust'], '株価': yahoo['price'], '配当利回り': yahoo['dividend'], '時価総額': yahoo['jikaso'], '発行株数': yahoo['hakokabu'], '配当1株': yahoo['dividend_1k'], 'PER': yahoo['PER'], 'PBR': yahoo['PBR'], 'EPS': yahoo['EPS'], 'BPS': yahoo['BPS'], '前日始値': yahoo['Open'], 'SELL_EVENT': row['SELL_EVENT'], '前日高値': yahoo['High'], '前日安値': yahoo['Low'], '出来高': yahoo['Volume'], '前前日始値': row['前日始値']}
            # 価格と単位のチェック
            try:
                int(yahoo['amount']/1)
                int(yahoo['price'])/1
            except:
                self.send_msg += str(row['コード']) + row['銘柄名'] + "_YAHOOから取得した単位、価格が不正です。" + "\n"
            # 日次株価一カ月更新
            dict_www = {}
            for i in range(25, 1, -1):
                dict_www['DAY' + str(i)] = row['DAY' + str(i - 1)]
            dict_www['DAY1'] = row['株価']
            dict_w.update(dict_www)
            try:
                npp = np.array(list(dict_www.values()))
                dict_w['乖離avg25'] = round(dict_w['株価'] / np.average(npp),4)
                max_w = npp.max()
                min_w = npp.min()
                dict_w['HighLow25'] = round((dict_w['株価'] - min_w) / (max_w - min_w), 2)

                dict_w['乖離avg5'] = round(dict_w['株価'] / np.average(npp[-5:]),4)
                max_w = npp[-5:].max()
                min_w = npp[-5:].min()
                dict_w['HighLow5'] = round((dict_w['株価'] - min_w) / (max_w - min_w), 2)
            except:
                pass
            sqls = common.create_update_sql(DB_BYBY, dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行

    def ment_week(self):
        sqls = "select *,rowid from kabu_list"
        sql_pd = common.select_sql(DB_BYBY, sqls)
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            code = row['コード']
            res = self.hist_check_week(DB_BYBY, code, row['rowid'])
            if res == -1:
                self.send_msg += str(code) + row['銘柄名'] + "のヒスト取得でエラーが発生しました。" + "\n"


    def ETF_check(self):
        # 全銘柄取得
        #        conn = sqlite3.connect(DB_BYBY)
        #        sqls = "select コード,セクタ from kabu_list"
        #        tsd = pd.read_sql(sqls,conn)
        #        conn.close()
        # ETF一覧取得
        base_url = "https://www.jpx.co.jp/equities/products/etfs/issues/01.html"
        tables = common.read_html2(base_url,0)

        eft_list = tables[0]['コード']

        for i in range(len(eft_list)):
            code = eft_list[i]
            dict_w = {'セクタ': "ETF"}
            common.create_update_sql(DB_BYBY, dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行


    def JREIT_check(self):
        # JREIT一覧取得
        UURL = "https://www.jpx.co.jp/equities/products/reits/issues/index.html"
        dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            aa = idx % 2
            if aa == 1:
                code = row[2][:4]
                sqls = "select コード,銘柄名,セクタ,rowid  from %(table)s where コード = '%(key1)s' and  (セクタ IS NULL or セクタ != '%(key2)s')" % {'table': 'kabu_list', 'key1': code, 'key2': "REIT"}
                yahoo3 = common.real_stock3(code)
                if len(yahoo3) > 0:
                    dict_w = {'セクタ': "REIT", '市場': yahoo3['市場名'], '日付': yahoo3['決算'], '単位': yahoo3['単元株数'].replace("株", "")}
                    common.create_update_sql(DB_BYBY, dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行

    def hibu_check4(self):
        dir_list = ["99_dict", "D_K_01_shin"]
        file_path = common.save_path(common.dir_join(dir_list), "shina.csv")
        # 逆日歩アップデート
        f = open(file_path, 'r', encoding="cp932")
        dataReader = csv.reader(f)
        for row in dataReader:
            common.to_number(row)
            if row[10] != '*****':
                dict = {'table': 'kabu_list', 'key1': row[2], 'key2': row[10], 'key3': common.env_time()[1]}
                sqls = "UPDATE %(table)s SET 逆日歩 = '%(key2)s','uptime' = '%(key3)s' where コード = '%(key1)s'" % dict
                common.sql_exec(DB_BYBY, sqls)

    def finance_update_week(self):
        # テーブル削除処理
        sqls = "DROP TABLE IF EXISTS finance;"
        common.sql_exec(DB_BYBY, sqls)
        # CSVファイル開く
        full_path = common.flag_path(common.RUBY_LIST, '_consolidate_con.txt')
        f = open(full_path, 'r')
        dataReader = csv.reader(f)
        header = next(dataReader)
        for row in dataReader:
            common.to_number(row)
            if len(row) == 59 and row[1] != "---":
                # ファイナンス情報のDBインポート
                finance_row = {'コード': "", '決算期': "", '会計方式': "", '決算発表日': "", '決算月数': "", '売上高': "", '営業利益': "", '経常利益': "",
                                '当期利益': "", 'EPS（一株当たり利益）': "", '調整一株当たり利益': "", 'BPS（一株当たり純資産）': "", '総資産': "", '自己資本': "", '資本金': "",
                                '有利子負債': "", '自己資本比率': "", 'ROA（総資産利益率）': "", 'ROE（自己資本利益率）': "", '総資産経常利益率': "", \
                                # 1期前
                                '決算期1': "", '会計方式1': "", '決算発表日1': "", '決算月数1': "", '売上高1': "", '営業利益1': "", '経常利益1': "", \
                                '当期利益1': "", 'EPS（一株当たり利益）1': "", '調整一株当たり利益1': "", 'BPS（一株当たり純資産）1': "", '総資産1': "", '自己資本1': "", '資本金1': "", \
                                '有利子負債1': "", '自己資本比率1': "", 'ROA（総資産利益率）1': "", 'ROE（自己資本利益率）1': "", '総資産経常利益率1': "", \
                                # 2期前
                                '決算期2': "", '会計方式2': "", '決算発表日2': "", '決算月数2': "", '売上高2': "", '営業利益2': "", '経常利益2': "", \
                                '当期利益2': "", 'EPS（一株当たり利益）2': "", '調整一株当たり利益2': "", 'BPS（一株当たり純資産）2': "", '総資産2': "", '自己資本2': "", '資本金2': "", \
                                '有利子負債2': "", '自己資本比率2': "", 'ROA（総資産利益率）2': "", 'ROE（自己資本利益率）2': "", '総資産経常利益率2': ""}
                n = 0
                for k in finance_row.keys():
                    finance_row[k] = row[n].replace("か月", "").replace("%", "").replace("円", "")
                    n += 1

                finance_row["決算月"] = finance_row["決算期"].replace("月期", "")[5:]
                common.insertDB3(DB_BYBY, 'finance', finance_row)

                # rowid取得
                sqls = "select *,rowid from %(table)s where コード = '%(key1)s' ;" % {'table': 'kabu_list','key1': row[0]}
                sql_pd = common.select_sql(DB_BYBY, sqls)
                if len(sql_pd) > 0:
                    del finance_row["コード"]
                    common.create_update_sql(DB_BYBY, finance_row, 'kabu_list', sql_pd['rowid'][0]) #最後の引数を削除すると自動的に最後の行

    def setup_basic_auth(self, base_uri, user, password):
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(realm=None,uri=base_uri,user=user,passwd=password)
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(auth_handler)
        urllib.request.install_opener(opener)

    def file_dawnload(self, url, file_path):
        try:
            urllib.request.urlretrieve(url, "{0}".format(file_path))
            f_size = os.path.getsize(file_path)
            if f_size == 0:
                self.send_msg += url + "が更新されてません。" + "\n"
        except:
            self.send_msg += "ダウンロード失敗:" + url + "\n"

    def hist_check_week(self, DB_BYBY, code, rid):
        try:
            dict_w = {}
            today = datetime.date.today()
            yest_day = today - datetime.timedelta(days=700)
            result, df = common.get_stock4(code, yest_day, today)
            if result != 1:
                return - 1
            print(code)
            for day in [700, 365, 180, 90, 30]:
                yest_day = today - datetime.timedelta(days=day)
                yest_day = yest_day.strftime("%Y/%m/%d")
                for ii in range(len(df)):
                    if str(yest_day) > str(df.index[ii])[:10].replace("-","/"):
                        break
                cnt = ii
                if cnt == 0:
                    continue
                d = df.index[:ii]
                o = df.l_O[:ii]
                h = df.l_H[:ii]
                l = df.l_L[:ii]
                c = df.l_C[:ii]
                v = df.l_V[:ii]
                a = df.l_AC[:ii]
                close_avg = int(sum([c[i] for i in range(len(c))]) / cnt)
                close_avg_vol = int(sum([c[i] for i in range(len(c))]) / cnt)
                dict_w['乖離avg' + str(day)] = round((c[0] / close_avg), 2)
                dict_w['HighLow' + str(day)] = round((c[0] - min(c)) / (max(c) - min(c)), 2)
                dict_w['乖離avg_vol_' + str(day)] = round((v[0] / close_avg_vol), 2)
                dict_w['HighLow_vol_' + str(day)] = round((v[0] - min(v)) / (max(v) - min(v)), 2)

                if day == 90:
                    vora = round((max(df.l_H) - min(df.l_L)) / c[0], 3)

                if day == 30:
                    dict_w['AVG20出来高'] = int(sum([v[i] for i in range(len(v))]) / cnt)
                    dict_w['AVG20出来高指数300以上OK'] = int(close_avg * dict_w['AVG20出来高'] / 1000000)

                    # high_log25,'key5':val_avg_kari25
                    #'HighLow25' = '%(key4)s','乖離avg25' =
            common.create_update_sql(DB_BYBY, dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行

            return 0
        except:
            self.send_msg += str(code) + "でエラーが発生しました。" + "\n"
            return -1

    def click365_info(self):
        table_name = "click365"
        dfs = common.click365()

        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            dict_w = {}
            for ii in range(len(row)):
                dict_w[dfs[0].columns[ii]] = row[ii]
            common.insertDB3(DB_BYBY, table_name, dict_w)

    def rating_check(self):  # 決算発表直
        UURL = "https://www.traders.co.jp/domestic_stocks/domestic_market/attention_rating/attention_rating.asp"
        dfs = pd.read_html(common.Chorme_get(UURL), header=0)

        # テーブル番号検索
        num = 0
        for ii in range(len(dfs)):
            if dfs[ii].columns[0] == "日付":
                num = ii
                break
        today = datetime.datetime.now().strftime("%m/%d")
        # 新規追加確認
        for idx, row in dfs[num].iterrows():
            # 当日確認
            if row[0] == today:
                # UPダウンの→チェック
                code = row['コード']
                if row[6].count("→"):
                    tmpsp = row[6].split("→")
                    sor = tmpsp[0]
                    dec = tmpsp[1].replace("円", "")
                    if sor < dec:
                        cnt = 1
                    if sor > dec:
                        cnt = -1
                    # DBのレーディング情報取得
                    sqls = "select *,rowid from %(table)s where コード = '%(key1)s'" % {'table': 'kabu_list', 'key1': code}
                    tsd = common.select_sql(DB_BYBY, sqls)
                    if len(tsd) > 0:
                        # RATING計算
                        try:
                            cnt = int(cnt) + int(tsd['レーディング'])
                        except:
                            pass
                        # RATING更新
                        common.create_update_sql(DB_BYBY, {'レーディング': cnt}, 'kabu_list', tsd['rowid'][0])  #最後の引数を削除すると自動的に最後の行

    def list_to_csv(self):
        code = ""
        sqls = "select *,rowid from kabu_list"
        sql_pd = common.select_sql(DB_BYBY, sqls)
        for i, row in sql_pd.iterrows():
            code += str(row['コード']) + "," + str(row['市場']) + "\n"
        filename = common.CODE_LIST
        common.create_file(filename, code, "utf-8")  # utf-8 cp932
        dir_list = ["99_dict", "W_K_01_kabu_list"]
        file_path = common.save_path(common.dir_join(dir_list), common.env_time()[0][:8] + "_kabu_list.CSV")
        sql_pd.to_csv(file_path, encoding="cp932")

    def gyoseki_db(self,rows):#業績DB作成
        sp = rows.split('@@')
        for key in ['連','単']:#連結が失敗した単で調査
            #空のデータフレームを作成
            df = pd.DataFrame(index=[], columns=['年次','売上高', '営業利益', '経常利益', '純利益', '1株益(円)', '1株配(円)'])
            sp.reverse()
            for i in range(len(sp)):#年次分データフレーム作成
                if sp[i].count(key):
                    sp_dital = common.to_number(list(sp[i].strip().replace(". ", ".").split(' ')))
                    df = df.append(pd.DataFrame([sp_dital], columns=df.columns))
            if len(df) > 0:#連結が成功チェック?
                break
        return df.reset_index(drop=True)

    def shiki_update(self):
        dict_w = {}
        sqls = "select *,rowid from kabu_list"
        sql_pd = common.select_sql(DB_BYBY, sqls)
        for i, row in sql_pd.iterrows():
            code = row['コード']
            sqls = "select *,rowid from %(table)s where コード = '%(key1)s';" % {'table': 'all_shiki','key1':code}
            sql_pd = common.select_sql('I04_shiki.sqlite', sqls)
            #0レコードはNG
            if len(sql_pd) < 2:
                continue
            num = len(sql_pd) - 1
            #当月の15-30のみ更新する。
            if sql_pd['日付'][num] != common.env_time()[0][0:6]:
                return 0

            for row_name in ['外国', '投信', '浮動株', '特定株']:
                try:
                    dict_w[row_name] = float(sql_pd[row_name][num])
                except:
                    dict_w[row_name] = 0

            df = self.gyoseki_db(sql_pd['【業績】'][num])  #業績DB作成
            if len(df) > 1:
                print(code)
                dict_w['kei_comp1'] = round(common.divide_Zero(df['経常利益'][0] , df['経常利益'][1]),2)
                dict_w['sel_comp1'] = round(common.divide_Zero(df['売上高'][0] , df['売上高'][1]),2)
                if len(df['経常利益']) > 2:
                    dict_w['kei_comp2'] = round(common.divide_Zero(df['経常利益'][1] , df['経常利益'][2]),2)
                    dict_w['sel_comp2'] = round(common.divide_Zero(df['売上高'][1] , df['売上高'][2]), 2)
                df2 = self.gyoseki_db(sql_pd['【業績】'][num - 1])  #前期からの収益変化率
                if len(df2) > 1:
                    for i in range(len(df)):
                        if df['年次'][i] == df2['年次'][0]:
                            dict_w['bef_comp'] = round(common.divide_Zero(df['経常利益'][i] , df2['経常利益'][0]), 2)
                            break

            #四季報経常利益戦略のフラグ
            res = self.shiki_plfit(code)
            if res > 0:
                dict_w['shiki_pf_L'] = res
            else:
                dict_w['shiki_pf_L'] = 0

            #四季報経常減益のフラグ 売り用
            res = self.shiki_plfit_sell(code)
            if res > 0:
                dict_w['shiki_pf_S'] = res
            else:
                dict_w['shiki_pf_S'] = 0
            sqls = common.create_update_sql(DB_BYBY, dict_w, 'kabu_list', code)  #最後の引数を削除すると自動的に最後の行

#            except:
#                self.send_msg += "四季UPDATEエラー" + str(code) + "\n"
#                pass

    def export_db(self):
        # 全テーブル情報取得
        CSV_DIR = r'E:\backup\SKYLINE\CSV'
        DB_DIR = os.path.join(os.environ["DROP"], r'servers\ARISTO\db')
        files = glob.glob(os.path.join(DB_DIR ,'*.sqlite'))
        for DB in files:
            sqls = "select name from sqlite_master where type='table'"
            sql_pd = common.select_sql(DB, sqls)

            for table_name in list(sql_pd['name']):
                sqls = "select * from %(table)s" % {'table': table_name}
                sql_pd2 = common.select_sql(DB, sqls)
                DB_file = os.path.basename(DB)
                file_path = os.path.join(CSV_DIR, DB_file, table_name + ".csv")
                common.create_dir(file_path)
                try:
                    sql_pd2.to_csv(file_path, encoding="cp932")  #,encoding="utf-8" , encoding="cp932"
                except:
                    sql_pd2.to_csv(file_path, encoding="utf-8")

    def PL_cale(self):
        # 全テーブル情報取得
        files = glob.glob(os.path.join(common.DB_DIR ,'*.sqlite'))
        for DB in files:
            common.sum_update(DB)

    def Foreigner(self):
        sqls = "select *,rowid from kabu_list where セクタ NOT IN ('ETF','REIT')"
        sql_pd = common.select_sql(DB_BYBY, sqls)
        dict_w = {}
        dict_w['時価総額ALL'] = 0
        dict_w['時価総額外人'] = 0
        dict_w['時価総額外人以外'] = 0
        dict_w['上昇率ALL'] = 0
        dict_w['上昇率外人'] = 0
        dict_w['上昇率外人以外'] = 0

        dict_w['count'] = 0
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            code = str(row['コード'])
            try:
                gaijin = float(row['外国']) / 100
                no_gaijin = 1 - gaijin
                vora = row['株価'] / row['DAY1'] -1
                dict_w['時価総額外人'] += row['時価総額'] * gaijin
                dict_w['時価総額外人以外'] += row['時価総額'] * no_gaijin
                dict_w['時価総額ALL'] += row['時価総額']
                dict_w['上昇率外人'] += row['時価総額'] * gaijin * vora
                dict_w['上昇率外人以外'] += row['時価総額'] * no_gaijin * vora
                dict_w['上昇率ALL'] += row['時価総額'] * vora
                dict_w['count'] += 1
            except:
                continue

        dict_w['加重平均外人'] = round(dict_w['上昇率外人'] / dict_w['時価総額外人'] * 100,3)
        dict_w['加重平均外人以外'] = round(dict_w['上昇率外人以外'] / dict_w['時価総額外人以外'] * 100,3)
        dict_w['加重平均ALL'] = round(dict_w['上昇率ALL'] / dict_w['時価総額ALL'] * 100,3)
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'rashio'}
        sql_pd = common.select_sql('I01_all.sqlite', sqls)
        dict_w['TOPIX_S15'] = sql_pd['TOPIX_S15'][0]
        dict_w['TOPIX_C15'] = sql_pd['TOPIX_C15'][0]
        dict_w['N225openD'] = sql_pd['N225openD'][0]
        dict_w['N225closeD'] = sql_pd['N225closeD'][0]
        #前日の値取得
        sqls = "select *,rowid from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': 'Forei'}
        sql_pd = common.select_sql(DB_BYBY, sqls)
        dict_w['TOPIX_C15_vora'] = round(dict_w['TOPIX_C15'] / sql_pd['TOPIX_C15'][0] -1 ,2)
        dict_w['N225closeD_vora'] = round(dict_w['N225closeD'] / sql_pd['N225closeD'][0] -1 ,2)

        # 投資部門別売買状況(週次)
        UURL = "https://stock-marketdata.com/total-trading-value-of-stocks.html"
        # テーブル情報取得
        dfs = common.read_html2(UURL, 1)  # header=0,skiprows=0(省略可能)
        title = ['期間', '投信','事業法人', 'その他','保険生保', '銀行','信銀', 'その他金融','現金個人', '信用個人','海外投資家']
        for i in range(len(dfs[0].columns)):
            dict_w[title[i]] = dfs[0].ix[1, i]
        common.insertDB3(DB_BYBY, 'Forei', dict_w)

    def shiki_plfit(self, code, datef=common.env_time()[0][0:6]):
#        if datef[4:6] == '06':
#            return -16
        try:
            sqls = "select 日付,【業績】,外国,貸借優待,rowid from %(table)s where コード = '%(key1)s' and 日付 <= '%(key2)s';" % {'table': 'all_shiki','key1':code,'key2':datef}
            sql_pd = common.select_sql('I04_shiki.sqlite', sqls)
            #10レコード以下はNG
            if len(sql_pd) < 5:
                return -11
            for cnt in [5, 1]:  #最新と１年前の業績比較
                flag = 0
                num = len(sql_pd) - cnt
                df = self.gyoseki_db(sql_pd['【業績】'][num])#業績DB作成
                if len(df) < 5:
                    return -12
                #データフレームを使ったデータのチェック
                for i in range(3):
                    if df['経常利益'][i] > df['経常利益'][i + 1] * 1.1 and df['売上高'][i] > df['売上高'][i + 1]:
                        pass
                    else:
                        return -13
                    #去年と今年の実績比較、去年より下がってたら除外
                    if ['年次'].count("予") == False and flag == 0:
                        flag = 1
                        if cnt == 5:
                            last_pl = df['経常利益'][i]
                        else:
                            if last_pl > df['経常利益'][i]:
                                return -14
                if cnt == 1:#最新の場合は2割りの伸び率が必要
                    if df['経常利益'][0] > df['経常利益'][1] * 1.2 and df['売上高'][0] > df['売上高'][1] * 1.1:
                        pass
                    else:
                        return -15
            return round(df['経常利益'][0] / df['経常利益'][1],3)
        except:
            return -999

    def shiki_plfit_sell(self, code, datef=common.env_time()[0][0:6]): #売り用
        try:
            sqls = "select 日付,【業績】,外国,貸借優待,rowid from %(table)s where コード = '%(key1)s' and 日付 <= '%(key2)s';" % {'table': 'all_shiki','key1':code,'key2':datef}
            sql_pd = common.select_sql('I04_shiki.sqlite', sqls)
            #10レコード以下はNG
            if len(sql_pd) < 5:
                return -11
            for cnt in [5, 1]:  #最新と１年前の業績比較
                flag = 0
                num = len(sql_pd) - cnt
                df = self.gyoseki_db(sql_pd['【業績】'][num])#業績DB作成
                if len(df) < 5:
                    return -12
                if cnt == 1:  #最新の場合は2割りの伸び率が必要
                    profit = round(df['経常利益'][0] / df['経常利益'][1],3)
                    if profit < 0.5 and df['売上高'][0] < df['売上高'][1]:
                        #外国>1,貸借あり
                        if sql_pd['貸借優待'][num].count('貸借') == False:
                            return -16
                        else:
                            pass
                    else:
                        return -15
            return profit
        except:
            return -999

    def shiki_hist2(self, t_date=201803):
        table_name = 'all_shiki'
        sqls = "select *,rowid from %(table)s where 日付 > %(key1)s" % {'table': table_name, 'key1': t_date}
        sql_pd = common.select_sql('I04_shiki.sqlite', sqls)

        #当月の15-30のみ更新する。
        num = len(sql_pd) - 1
        if sql_pd['日付'][num] != common.env_time()[0][0:6]:
            return 0

        sys.path.append(common.PROFIT_DIR)
        import common_profit as compf
        sqls = "select *,rowid from %(table)s" % {'table': 'kabu_list'}
        sql_pd = common.select_sql('B01_stock.sqlite', sqls)

        for i, row in sql_pd.iterrows():
            dict_w = {}
            code = row['コード']
            code_text = common.save_path(common.RUBY_DATA, "jp")
            code_text = common.save_path(code_text, str(code)) + ".txt"
            if os.path.exists(code_text):
                df = pd.DataFrame(index=pd.date_range('2007/01/01', common.env_time()[1][0:10]))
                df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
                df = df.dropna()
                df.columns = ['O', 'H', 'L', 'C', 'V', 'C2', 'SS'][:len(df.columns)]
                try:
                    df = compf.add_avg_rng(df, 'C', 'L', 'H')
                except:
                    continue
                y = df.dropna()
                if len(df.columns) != 15:
                    continue

                sqls = "select *,rowid from %(table)s where コード = '%(key1)s' and 日付 >= '%(key2)s'" % {'table': 'all_shiki','key1': code,'key2': t_date}
                sql_pd2 = common.select_sql('I04_shiki.sqlite', sqls)
                if len(sql_pd2) == 0:
                    continue

                for ii, row2 in sql_pd2.iterrows():
                    dict_w = {}
                    day_w = datetime.date(int(row2['日付'][:4]), int(row2['日付'][4:]), 15)
                    day_e = day_w + datetime.timedelta(days=90)

                    dd = pd.DataFrame(index=pd.date_range(row2['日付'][:4] + '/' + row2['日付'][4:] + '/15', day_e))
                    dd = dd.join(df).dropna()
                    if len(dd) == 0:
                        continue
                    dict_w['rng7'] = dd['rng7'][0]
                    dict_w['rng30'] = dd['rng30'][0]
                    dict_w['rng200'] = dd['rng200'][0]
                    dict_w['rng365'] = dd['rng365'][0]
                    dict_w['avg7'] = dd['avg7'][0]
                    dict_w['avg30'] = dd['avg30'][0]
                    dict_w['avg200'] = dd['avg200'][0]
                    dict_w['avg365'] = dd['avg365'][0]

#                    num = len(dd) -1
                    dict_w['日付1'] = str(dd.index[0])[:10].replace("-","/")
                    dict_w['始値1'] = dd['C'][0]
                    dict_w['日付2'] = str(dd.index[-1])[:10].replace("-","/")
                    dict_w['始値2'] = dd['C'][-1]
                    try:
                        dict_w['PL_始値'] = round(int(dict_w['始値2']) / int(dict_w['始値1']) - 1, 2)
                    except:
                        pass
                    sqls = common.create_update_sql('I04_shiki.sqlite', dict_w, 'all_shiki', row2['rowid'])  #最後の引数を削除すると自動的に最後の行
        #平均の増減値取得
        SHIKI_DB = 'I04_shiki.sqlite'
        table_byby = "avg_all"
        sqls = "delete from %(table)s where rowid > 0" % {'table': table_byby}
        common.sql_exec(SHIKI_DB, sqls)
        sqls = "select 日付,avg(PL_始値) as avg_all,count(*) as cnt from %(table)s GROUP BY 日付" % {'table': table_name}
        sql_pd = common.select_sql(SHIKI_DB, sqls)
        for i, row in sql_pd.iterrows():
            dict_w = {'日付':row['日付'],'avg':round(row['avg_all'],3),'cnt':row['cnt']}
            common.insertDB3(SHIKI_DB, table_byby, dict_w)

    def table_update_check(self): #nowカラムで更新テーブルを確認する
        # 全テーブル情報取得
        msg = ""
        files = glob.glob(os.path.join(common.DB_DIR ,'*.sqlite'))
        for DB in files:
            DB_file = os.path.basename(DB)
            if DB_file in ('I04_shiki.sqlite','●shiki_work.sqlite'): #除外DBはここに追加
                continue
            sqls = "select name from sqlite_master where type='table'"
            sql_pd = common.select_sql(DB, sqls)

            for table_name in list(sql_pd['name']):
                if table_name in ('wait_list','stock_list','kabu_list','retry','poji_fx') or table_name.count('_archive'): #除外テーブルをここに追加
                    continue
                sqls = "select now from %(table)s where rowid=(select max(rowid) from %(table)s) ;" % {'table': table_name}
                sql_pd = common.select_sql(DB, sqls)
                if len(sql_pd) != 0:
                    past_day = -10
                    if DB_file in ('I03_Monthly.sqlite') or table_name in ('ano_check','event_stocks_exchange','event_name_change','event_union','MARKET_EXIT'):#70日以上古い場合
                        past_day = -70
                    if common.date_diff(sql_pd['now'][0]) < past_day:
                        msg += "更新日時:" + sql_pd['now'][0] + "  DB:" + DB_file + "  table_name:" + table_name + "\n"
        if msg != "":
            common.mail_send(u'テーブル更新頻度チェック', msg)

    def export_db2(self):
        sql_pd = common.select_sql('B01_stock.sqlite', "select * from kabu_list")
        file_path = common.save_path(common.dir_join(["99_dict", "D_kabu_list"]), common.env_time()[0][:8] + "_kabu_list.csv")
        sql_pd.to_csv(file_path, encoding="cp932")  #,encoding="utf-8" , encoding="cp932"


if __name__ == '__main__':
    info = k51_DBupdate()
    argvs = sys.argv
    today = datetime.date.today()
    if argvs[1] == "mon7": #710
        info.click365_info()
        info.export_db2()

    if argvs[1] == "export":#Windowsのみ CSVへ出力
        info.export_db()

    if argvs[1] == "day": #1900
        info.main()  # コードから上場区分
        info.main2()  # 信用区分 申込停止
        info.ment3()  # yahoo株価情報更新
        info.hibu_check4()  # 逆日歩チェック
        info.Foreigner()  #外国人の売買動向チェック(※yahoo株価情報更新が終わっていること)
        info.PL_cale()
        info.ment_week()  # yahoo株価情報更新(週次)
        try:
            info.rating_check()  # レーティング情報取得 #www.traders.co.jp
        except:
            info.send_msg += "tradersエラー" + __file__ + "\n"
        if info.send_msg != "":
            text = "前営業日:" + common.last_day() + "\n" + "翌営業日:" + common.next_day() + "\n"
            common.mail_send(u'DBメンテ終了' + argvs[1], text + info.send_msg)

    if argvs[1] == "week1" and common.week_end_day() == 1:
        info.list_to_csv()
        info.ETF_check()  # ETFセクタ更新
        info.JREIT_check()
        info.finance_update_week()

        info.table_update_check()
        info.shiki_update()
        info.shiki_hist2()

        common.mail_send(u'DBメンテ終了週次1' + argvs[1], info.send_msg)

#    if argvs[1] == "week2" and common.week_end_day() == 1:
#        info.ment_week()  # yahoo株価情報更新(週次)
#        info.shiki_update()
#        info.shiki_hist2()
#        common.mail_send(u'DBメンテ終了週次2' + argvs[1], info.send_msg)

    print("end", __file__)
