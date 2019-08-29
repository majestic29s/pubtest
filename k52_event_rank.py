# -*- coding: utf-8 -*-

import sqlite3
import pandas as pd
import requests
import datetime
import csv
import os
import sys
import glob
from bs4 import BeautifulSoup
from time import sleep
import time
import shutil
from dateutil.relativedelta import relativedelta
import numpy as np

# 独自モジュールインポート
import common
sys.path.append(common.LIB_DIR)
import k93_smbc

sys.path.append(common.PROFIT_DIR)
import common_profit as compf

DB = common.save_path('I02_event.sqlite')


class k52_event_rank:
    def __init__(self, num):
        self.num = num
        self.today = datetime.date.today()
        self.error_msg = ""
        self.send_msg = ""

    def yahoo_daily(self):
        AA = ["27", "28", "29", "30", "1", "2","3", "13", "14", "57", "58", "56"]
        BB = ["ストップ高", "ストップ安", "年初来高値更新", "年初来安値更新", "値上がり率", "値下がり率","出来高", "信用買残増加", "信用売残増加", "検索数", "検索率上昇", "掲示板投稿数"]
        DD = ["stophigh", "stoplow", "year_high", "year_low", "Pricerate", "Pricerate","Pricerate", "Creditremaining", "Creditremaining", "Search", "Search", "Search2"]

        T_DAY = common.env_time()[0][0:8]

        for i in range(len(AA)):
            print(BB[i])
            UURL = "https://info.finance.yahoo.co.jp/ranking/?kd=" + AA[i] + "&tm=d&vl=a&mk=1&p=a"
#            df = ""
            print(UURL)
            try:
                if BB[i] in ["ストップ高", "ストップ安", "年初来高値更新", "年初来安値更新"]:
                    for ii in range(10):
                        UURL = "https://info.finance.yahoo.co.jp/ranking/?kd=" + AA[i] + "&tm=d&vl=a&mk=1&p=" + str(ii+1)
                        # header=0,skiprows=0(省略可能)
                        dfs = common.read_html2(UURL, 0)
                        if len(dfs[0]) == 1:
                            break
                        elif ii == 0:
                            df = dfs[0]
                        else:
                            # 結合処理追加 contact
                            df = pd.concat([df, dfs[0]])
                else:
                    # header=0,skiprows=0(省略可能)
                    dfs = common.read_html2(UURL, 0)
                    df = dfs[0]

                if len(df) > 1:
                    for idx, row in df.iterrows():
                        common.to_number(row)
                        if len(str(row["コード"])) != 4:
                            continue
                        data = row.to_dict()
                        data['DAY'] = T_DAY
                        col_name = {}
                        col_name['タイトル'] = BB[i]
                        col_name = {**col_name, **data}
                        col_name = common.add_dict(row["コード"], col_name)
                        common.insertDB3(DB, "YAHOO_" + DD[i], col_name)
            except:
                if BB[i] != 'ストップ安':
                    self.send_msg += BB[i] + "更新中にエラー発生"

    def Title_check(self, key):
        head = {'No.': "", 'ｺｰﾄﾞ': "", '銘柄名': "", '市場': "", '信用': "",'日付': "", '時刻': "", '価格': "", '前日比': "", '(%)': "", '出来高(株)': ""}
        if key == "ohfuku":
            add_col = {'業種': "", '過去2日3%往復回数': "",'平均売買代金(100万円)': "", '時価総額(10億円)': "", '60日ﾎﾞﾗﾃｨﾘﾃｨ(％)': ""}
        elif key == "trend":
            add_col = {'業種': "", 'シャープレシオ': "", '平均売買代金(100万円)': ""}
        elif key == "gap":
            add_col = {'ギャップ(%)': "", '寄付き後変動率(%)': ""}
        elif key == "hige":
            add_col = {'過去3日間上ヒゲ(%)': "", '過去3日間下ヒゲ(%)': ""}
        elif key == "plus":
            add_col = {'売買代金': "", '過去5日間陽線数': ""}
        elif key == "minus":
            add_col = {'売買代金': "", '過去5日間陰線数': ""}
        elif key == "stop":
            add_col = {'業種': "", '前日終値': "", '本日の値幅(円)': "", '値幅充足率(％)': ""}
        elif key == "hori_rnk":
            add_col = {'寄付き後変動率(％)': "", '平均売買代金(100万円)': ""}
        elif key == "volume":
            add_col = {'当日売買代金(100万円)': "", '平均売買代金(100万円)': ""}
        else:
            return {'DUMMY': ""}

        for kkk, vvv in add_col.items():
            head[kkk] = vvv
        tail = {'時価総額(10億円)': "", '60日ﾎﾞﾗﾃｨﾘﾃｨ(％)': ""}
        for kkk, vvv in tail.items():
            head[kkk] = vvv
        aaa = [vv for vv in head.keys()]
        return aaa

    def Title_weekly(self, key):
        head = ['コード', '銘柄名', '市場', 'rank', 'タイトル', '株価', 'DAY']
        if key == "half":
            add_col = ["対中間期進捗率", "５年平均進捗率"]
        elif key == "yubo":
            add_col = ["対通期進捗率", "５年平均進捗率"]
        elif key == "max":
            add_col = ["今期経常増益率", "今期経常利益"]
        elif key == "max_contune":
            add_col = ["最高益連続期数", "今期経常増益率"]
        elif key == "max_comeback":
            add_col = ["最高益間隔期数", "今期経常増益率"]
        elif key == "max_sales":
            add_col = ["営業増益率", "増益連続期数"]
        elif key == "base_sales":
            add_col = ["増益連続期数", "営業増益率"]
        elif key == "max_roe":
            add_col = ["今期ROE(予想)", "ROE上昇幅"]
        elif key == "roe_cntune":
            add_col = ["ROE向上連続期数", "今期ROE(予想)"]
        else:
            return ['DUMMY']
        head.extend(add_col)
        tail = ['決算期間', 'ＰＥＲ', 'ＰＢＲ', '利回り']
        head.extend(tail)

        return head

    def kabumap_daily(self):
        AA = ["techSig/base&dir=desc&kind=goback", "techSig/base&kind=sharp&dir=up", "techSig/base&kind=sharp&dir=down", "gugd/base&mode=1", "gugd/base&mode=2","hige/base&item=7", "hige/base&item=8", "yosen/base&mode=1", "yosen/base&mode=2", "stopStock/base&stop=0&sort=d&lim=4", "stopStock/base&stop=2&sort=u&lim=4","change/base", "volume/base&mode=1", "volume/base&mode=2"]
        BB = ["株価往復銘柄", "上昇トレンド", "下降トレンド", "ギャップアップ", "ギャップダウン", "上ヒゲ(%)", "下ヒゲ(%)","陽線", "陰線", "もうすぐストップ高", "もうすぐストップ安", "寄付後変動率ランキング", "出来高急上昇", "出来高急下降"]
        DD = ["ohfuku", "trend", "trend", "gap", "gap", "hige", "hige","plus", "minus", "stop", "stop", "hori_rnk", "volume", "volume"]
        T_DAY = common.env_time()[0][0:8]

        for i in range(len(AA)):
            #            Title = self.Title_check(DD[i])
            UURL = "https://dt.kabumap.com/servlets/dt/Action?SRC=" + AA[i]
            df = ""
            dfs = common.read_html2(UURL, 1, 0)  # header=0,skiprows=0(省略可能)
            df = dfs[0]
            if len(df) > 1:
                for idx, row in df.iterrows():
                    data = row.to_dict()
                    if data['No.'] == '日付':
                        continue
                    table_name = "map_" + DD[i]
                    data['タイトル'] = BB[i]
                    data['DAY'] = T_DAY
                    if table_name in ('map_minus', 'map_plus', 'map_stop','map_volume','map_hige','map_gap','map_trend','map_ohfuku','map_hori_rnk'):
                        row_code = 'ｺｰﾄﾞ'
                    else:
                        row_code = 'コード'

                    data = common.add_dict(data[row_code], data)
                    common.insertDB3(DB, table_name, data)

    def main_weekly(self):
        AA = ["funda_06&market=0&stc=&stm=0&page=", "funda_07&market=0&stc=&stm=0&page=", "funda_04&market=0&stc=&stm=0&page=", "funda_05&market=0&stc=&stm=0&page=",
              "f_T-y_Max-eki%26z-eki_Best100&market=0&stc=&stm=0&page=", "f_T-y_Max-eki_5k-rz&market=0&stc=&stm=0&page=", "f_T-y_Max-eki_5k-br&market=0&stc=&stm=0&page=",
              "funda_01&market=0&stc=&stm=0&page=", "funda_02&market=0&stc=&stm=0&page=", "f_T-y_ROE_best100&market=0&stc=&stm=0&page=",
              "f_T-y_ROE_5k-rz&market=0&stc=&stm=0&page="]
        BB = ["中間期上振れ_有望銘柄", "【第1四半期】時点_通期上振れ 有望銘柄", "【中間期】時点_通期上振れ 有望銘柄", "【第3四半期】時点_通期上振れ 有望銘柄", "最高益を見込む【増益率】ベスト100", "【連続最高益】銘柄リスト", "最高益“大復活”銘柄リスト",
              "【営業増益率】ベスト100", "【強固な収益基盤】銘柄リスト", "今期【高ROE】ベスト100", "【経営効率化が続く】銘柄リスト"]
        DD = ["half", "yubo", "yubo", "yubo", "max", "max_contune",
              "max_comeback", "max_sales", "base_sales", "max_roe", "roe_cntune"]
        T_DAY = common.env_time()[0][0:8]

        for i in range(len(AA)):
            data = {}
            FLAG = 0
            Title = self.Title_weekly(DD[i])

            # テーブル番号チェック
            UURL = "https://kabutan.jp/tansaku/?mode=1_" + AA[i] + str(1)
            dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
            table_No = None
            for ii in range(len(dfs)):
                if dfs[ii].columns[0] == 'コード':
                    table_No = ii
            if table_No == None:
                continue

            for ii in range(8):  # 8へ戻す
                UURL = "https://kabutan.jp/tansaku/?mode=1_" + AA[i] + str(ii+1)
                dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
                df_temp = dfs[table_No]
                if len(df_temp) <= 1:
                    break
                df_temp.columns = Title
                if FLAG == 0:
                    df = df_temp
                    FLAG = 1
                else:
                    df = pd.concat([df, df_temp])
            cnt = 0
            if len(df) > 1:
                for idx, row in df.iterrows():
                    data = row.to_dict()
                    cnt += 1
                    data['rank'] = cnt
                    data['タイトル'] = BB[i]
                    data['DAY'] = T_DAY
                    data = common.add_dict(row["コード"], data)
                    common.insertDB3(DB, "W_kabutan_" + DD[i], data)

    def event_check(self):
        AA = ["株式交換", "第三者割当増資", "公募・売出", "銘柄異動", "立会外分売", "社名変更", "株式分割", "株式併合", "業績修正"]
        DD = ["stocks_exchange", "third_person", "sale","brand_move", "tachi", "name_change", "split","union","achievements"]
        BB = ["https://www.traders.co.jp/domestic_stocks/stocks_data/stocks_exchange/stocks_exchange.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/third_person/third_person.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/sale/sale.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/brand_move/brand_move.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/tachi/tachi.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/name_change/name_change.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/stocks_split/stocks_split.asp",
            "https://www.traders.co.jp/domestic_stocks/stocks_data/stocks_union/stocks_union.asp",
            "https://www.traders.co.jp/domestic_stocks/domestic_market/achievements/achievements.asp"]

        CC = [['株式交換日', 'コード', '親市場', '親銘柄', '子コード', '子市場', '子銘柄', '交換比率'],
            ['発表日', 'コード', '市場', '銘柄', '枚数', '価格', '備考'],
            ['受渡日', '価格決定日', 'コード', '銘柄', '市場', '主幹事','公募', '売り出し', 'OA', '発行価格', '仮条件', '割引率'],
            ['上場日', 'コード', '銘柄', '既市場', '新市場', '備考'],
            ['実施日', '分売予定期間', 'コード', '銘柄', '市場','分割価格', '割引率', '予定株数', '実施株数', '申込上限'],
            ['変更日', 'コード', '市場', '新社名', '旧社名'],
            ['権利取最終日', 'コード', '市場', '銘柄', '比率', '効力発生日'],
            ['権利取最終日', 'コード', '市場', '銘柄', '比率', '効力発生日'],
            ['発表日', 'コード', '市場', '銘柄', '前回見通し', '今回見通し', '決算期', '変更箇所', '上下']]

        for i in range(0, len(AA)):
            table_name = "event_"+DD[i]
            UURL = BB[i]
            dfs = common.read_html2(common.Chorme_get(UURL), 0)  # header=0,skiprows=0(省略可能)
            for ii in range(len(dfs)):
                # テーブル番号検索
                if dfs[ii].columns[0] == CC[i][0]:
                    break
            # カラムの入れ替え
            col_name = {}
            col_name = {dfs[ii].columns[c]: CC[i][c]
                        for c in range(len(dfs[ii].columns))}
            dfs[ii] = dfs[ii].rename(columns=col_name)
            # 重複コード古いもの順に削除
            df = dfs[ii].drop_duplicates(['コード'], keep='first')
            # 株式交換は１行目削除
            if i == 0:
                df = df.drop(0)
            for idx, row in df.iterrows():
                dict_w = {}
                for ii in range(len(CC[i])):
                    dict_w[CC[i][ii]] = row[ii]

                self.event_check_exec(dict_w, table_name, AA[i])
                # 30以上はスキップ
                if idx > 100:
                    break

    def event_check_exec(self, dict_w, table_name, event):
        common.to_str(dict_w)
        if event == "公募・売出":
            dict_w['コード'] = dict_w['コード'][:4]

        code = dict_w['コード']
        # コードが正常ない場合は処理しない。
        if len(str(code)) != 4:
            return
        today = datetime.datetime.now()
        yest_day = today - datetime.timedelta(days=300)
        yest_day = yest_day.strftime("%Y/%m/%d %H:%M:%S")

        sqls = "select " + ",".join(dict_w.keys()) + ",rowid from %(table)s where コード = '%(key1)s' and now > '%(key2)s' and \
        rowid in ( select max(rowid) from %(table)s where コード = '%(key1)s') " % {'table': table_name, 'key1': str(code), 'key2': yest_day}
        sql_pd = common.select_sql(DB, sqls)
        # 新規tsd["コード"]
        if len(sql_pd) == 0:
            if event == "銘柄異動":
                work_day = today.strftime("%m/%d")
                work_year = today.strftime("%Y")
                if str(dict_w['上場日']) > str(work_day):
                    dict_w['上場日'] = str(work_year) + r"/" + str(dict_w['上場日'])
                else:
                    work_year = int(work_year) + 1
                    dict_w['上場日'] = str(work_year) + r"/" + str(dict_w['上場日'])
            dict_w = common.add_dict(code, dict_w)
            t_work = [str(i) for i in dict_w.values()]
            self.send_msg += "●" + event + "_new_" + "_".join(t_work) + "\n"
#            self.send_msg += "●" + event + "_new_" + "_".join(str(dict_w.values())) + "\n"
            common.insertDB3(DB, table_name, dict_w)
            # マスター一覧を更新
            if event in ["第三者割当増資", "公募・売出", "立会外分売"]:
                sqls = common.create_update_sql('B01_stock.sqlite', {"SELL_EVENT": common.env_time()[1]}, 'kabu_list', code) #最後の引数を削除すると自動的に最後の行
        # 更新あり
        else:
            dict_t = {}
            for v in range(len(sql_pd.columns)):
                dict_t[sql_pd.columns[v]] = sql_pd.ix[0, sql_pd.columns[v]]
            common.to_str(dict_t)
            t_id = dict_t["rowid"]
            # 比較前に条件がある場合は以下に追加
            del dict_t["rowid"]
            if event == "銘柄異動":
                dict_t["上場日"] = dict_t["上場日"][-5:]
            list_w = [str(tt) for rr, tt in dict_w.items()]
            list_t = [str(tt) for rr, tt in dict_t.items()]
            # 同じ場合はスキップ
            if list_w == list_t:
                return 0
            else:
                dict_a = {}
                for key in dict_w.keys():
                    if key == '前日':
                        break
                    if dict_w[key] == dict_t[key] or dict_w[key] == dict_t[key] + '.0' or dict_w[key] == dict_t[key] + '0' or dict_w[key] + '.0'== dict_t[key] :
                        pass
                    else:
                        dict_a[key] = dict_w[key]
                if len(dict_a) > 0:
                    tsd = common.kabu_search(code)
                    if len(tsd) > 0:
                        self.send_msg += "●" + event + "_update_" + tsd['銘柄名'] + "_".join(dict_a.values()) + "\n"
                        sqls = common.create_update_sql(DB, dict_a, table_name, t_id)  #最後の引数を削除すると自動的に最後の行
                    else:
                        sqls = "delete from %(table)s where rowid = '%(key1)s'" % {'table': table_name, 'key1': t_id}
                        common.sql_exec(DB, sqls)
            return 0

    def minkabu_daily(self):
        AA = ["stock/4/1/1/0/1", "stock/5/1/1/0/1","stock/59/1/1/0/1", "stock/60/1/1/0/1", "stock/63/1/1/0/1"]
        BB = ["買い予想数上昇ランキング", "売り予想数上昇ランキング","買い予想総数ランキング", "売り予想総数ランキング", "出来高変化率ランキング"]
        DD = ["buy", "sell", "buy_cnt", "sell_cnt", "volume_henka"]

        T_DAY = common.env_time()[0][0:8]

        for i in range(len(AA)):
            #            Title = self.Title_check(DD[i]) https://minkabu.jp/ranking/stock/60/1/1/0/1
            UURL = "https://minkabu.jp/ranking/" + AA[i]
            df = ""
            print(UURL)
            dfs = common.read_html2(UURL, 0, 0)  # header=0,skiprows=0(省略可能)
            df = dfs[0]
            if len(df) > 1:
                for idx, row in df.iterrows():
                    data = {}
                    if row["銘柄名"].count("位") == False:
                        continue
                    if BB[i] in ["買い予想数上昇ランキング", "売り予想数上昇ランキング"]:
                        bbb = row['目標株価'].split(r"(")
                        data['株価'] = bbb[0]
                        ccc = row['Unnamed: 3'].split(r"  ")
                        data['目標株価'] = ccc[0]
                        if len(ccc) != 1:
                            data['売買予測'] = ccc[1]
                    else:#"買い予想総数ランキング", "売り予想総数ランキング", "出来高変化率ランキング"
                        ccc = row['Unnamed: 4'].split(r"  ")
                        data['目標株価'] = ccc[0]
                        if len(ccc) != 1:
                            data['売買予測'] = ccc[1]
                        if DD[i] == "buy_cnt":
                            bbb = row['買い予想総数'].split(r"(")
                            data['買い予想総数'] = row['目標株価']
                        if DD[i] == "sell_cnt":
                            bbb = row['売り予想総数'].split(r"(")
                            data['売り予想総数'] = row['目標株価']
                        if DD[i] == "volume_henka":
                            bbb = row['出来高変化率'].split(r"(")
                            data['出来高変化率'] = row['目標株価']
                        data['株価'] = bbb[0]

                    data['順位'] = row["銘柄名"].replace("位","")
                    aaa = row['現在値'].split(r"  ")
                    data['コード'] = aaa[0]
                    data['銘柄名称'] = aaa[1]
                    data['タイトル'] = BB[i]
                    data['DAY'] = T_DAY
                    data = common.add_dict(data['コード'], data)
                    common.insertDB3(DB, "MINkabu_" + DD[i], data)
    def after_update(self):
        msg = ''
        # 全テーブル情報取得
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(DB, sqls)
        for i, rrow in sql_pd.iterrows():
            table_name = rrow['name']
            # 前日新規イベントの株価更新
            sqls = "select *,rowid from %(table)s where 当日 = '' or 当日 IS NULL " % {'table': table_name}
            sql_pd = common.select_sql(DB, sqls)
            if len(sql_pd) == 0:
                continue
            print("全テーブル更新",table_name,"行数",len(sql_pd))
            for iii, row in sql_pd.iterrows():
                common.to_number(row)
                code = row['コード']
                if code == 'nan':
                    continue
                # 株価の 取得(銘柄コード, 開始日, 終了日)

                tsd = common.kabu_search(code)
                if len(tsd) > 0:
                    dict_w = {}
                    if row['前日'] is None or row['前日'] == "":
                        dict_w['前日'] = tsd['uptime'][0:10]
                        dict_w['前日始値'] = tsd['前日始値']
                        dict_w['前日終値'] = tsd['株価']
                    else:
                        dict_w['当日'] = tsd['uptime'][0:10]
                        dict_w['当日始値'] = tsd['前日始値']
                        dict_w['当日終値'] = tsd['株価']
                    sqls = common.create_update_sql(DB, dict_w, table_name, row['rowid'])
                elif table_name != 'MARKET_EXIT':
                    sqls = "select *,rowid from %(table)s where コード = '%(key1)s'" % {'table': 'MARKET_EXIT', 'key1': code}
                    sql_pd2 = common.select_sql(DB, sqls)
                    if len(sql_pd2) > 0:
                        dict_w['当日'] = sql_pd2['当日'][0]
                        dict_w['当日始値'] = sql_pd2['当日始値'][0]
                        dict_w['当日終値'] = sql_pd2['当日終値'][0]
                        sqls = common.create_update_sql(DB, dict_w, table_name, row['rowid'])
                    else:
                        msg += table_name + "_:" + str(code) + "\n"
                        sqls = "delete from %(table)s where rowid = '%(key1)s'" % {'table': table_name, 'key1': row['rowid']}
                        common.sql_exec(DB, sqls)
        if msg != '':
            common.mail_send(u'カラム削除リスト', msg)


    def event_koukai(self): #信用取引に関する日々公表等
        table_name = "HIBIKOUKAI"
        # 解除後の翌日値挿入
        sqls = "select コード,rowid from %(table)s where 解除日翌日値 ='99'" % {'table': table_name}
        sql_pd = common.select_sql(DB, sqls)
        if len(sql_pd) > 0:
            for i, rrow in sql_pd.iterrows():
                code = rrow['コード']
                yahoo = common.real_stock2(code)
                dict_w = {'解除日翌日値': yahoo['Open']}
                common.create_update_sql(DB, dict_w, table_name, rrow['rowid'])
        UURL = "https://www.jpx.co.jp/markets/equities/margin-daily/index.html"
        dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
        # 解除確認
        sqls = "select コード,rowid from %(table)s where 解除日=''" % {'table': table_name}
        sql_pd = common.select_sql(DB, sqls)
        if len(sql_pd) > 0:
            for i, rrow in sql_pd.iterrows():
                code = rrow['コード']
                flag = 0
                for idx, row in dfs[1].iterrows():
                    code2 = row['コード']
                    if str(code) == str(code2):
                        flag = 1
                        break
                # 1は存在するため指定解除
                if flag == 1:
                    yahoo = common.real_stock2(code)
                    dict_w = {'解除日': row['解除日'], '解除日値': yahoo['price'], '解除日翌日値':'99'}
                    common.create_update_sql(DB, dict_w, table_name, rrow['rowid'])
        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            dict_w = {}
            print(row['指定日'] , common.last_day())
            if row['指定日'] < common.last_day():
                break
            for ii in range(len(row)):
                dict_w[dfs[0].columns[ii]] = row[ii]

            sqls = "select コード,rowid from %(table)s where コード = '%(key1)s' and 指定日 == '%(key2)s'" % {'table': table_name, 'key1': dict_w['コード'], 'key2': dict_w['指定日']}
            sql_pd = common.select_sql(DB, sqls)
            if len(sql_pd) > 0:
                continue
            else:
                dict_w = common.add_dict(dict_w['コード'], dict_w)
                dict_w['解除日'] = ""
                dict_w['解除日翌日値'] = ""
                common.insertDB3(DB, table_name, dict_w)

    def market_exit_(self):
        table_name = "MARKET_EXIT"
        UURL = "https://www.jpx.co.jp/listing/stocks/delisted/"
        dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)

        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            dict_w = {}
            for ii in range(len(row)):
                dict_w[dfs[0].columns[ii]] = row[ii]
            sqls = "select コード,rowid from %(table)s where コード = '%(key1)s' and 上場廃止日 = '%(key2)s'" % {'table': table_name, 'key1': dict_w['コード'], 'key2': dict_w['上場廃止日']}
            sql_pd = common.select_sql(DB, sqls)
            if len(sql_pd) > 0:
                continue
            else:
                dict_w = common.add_dict(dict_w['コード'], dict_w)
                common.insertDB3(DB, table_name, dict_w)

    def pts_night(self):
        table_name = "PTS_NIGTH"
        UURL = "https://www.morningstar.co.jp/StockInfo/pts/ranking?kind=2&page=0"
        dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
        dfs[0] = dfs[0].rename(columns={'基準値比': '現在値P', '出来高': '基準値比', '売買代金': '基準値比P', 'Unnamed: 8': '出来高', 'Unnamed: 9': '売買代金'})
        # 新規追加確認
        for idx, row in dfs[0].iterrows():
            dict_w = {}
            for ii in range(len(row)):
                try:
                    dict_w[dfs[0].columns[ii]] = row[ii].replace("%","")
                except:
                    dict_w[dfs[0].columns[ii]] = row[ii]

            dict_w = common.add_dict(dict_w['コード'], dict_w)
            common.insertDB3(DB, table_name, dict_w)

    def adr_night(self):
        table_name = "ADR_NIGTH"
        UURL = "https://www.traders.co.jp/foreign_stocks/adr.asp"
        dfs = common.read_html2(common.Chorme_get(UURL), 0)  # header=0,skiprows=0(省略可能)
        # テーブル番号検索
        num = 0
        for ii in range(len(dfs)):
            if dfs[ii].columns[0] == "コード":
                num = ii
                break
        # 新規追加確認
        for idx, row in dfs[num].iterrows():
            dict_w = {}
            for ii in range(len(row)):
                dict_w[dfs[num].columns[ii]] = row[ii]
            dict_w = common.add_dict(dict_w['コード'], dict_w)
            common.insertDB3(DB, table_name, dict_w)

    def kessan_last(self):  # 決算発表直
        table_name = "kessan_last"
        UURL = "https://www.traders.co.jp/domestic_stocks/domestic_market/achievements/achievements.asp"
        dfs = common.read_html2(common.Chorme_get(UURL), 0)  # header=0,skiprows=0(省略可能)

        # テーブル番号検索
        num = 0
        for ii in range(len(dfs)):
            if dfs[ii].columns[0] == "発表日":
                num = ii
                break
        today = datetime.datetime.now().strftime("%m/%d")
        # 新規追加確認
        for idx, row in dfs[num].iterrows():
            dict_w = {}
            # 決算日確認
            if row[0] != today:
                break
            for ii in range(len(row)):
                dict_w[dfs[num].columns[ii]] = row[ii]
            dict_w = common.add_dict(dict_w['コード'], dict_w)
            common.insertDB3(DB, table_name, dict_w)

    def ipo_chek(self):  # 決算発表直
        table_name = "ipo"
        UURL = "https://www.traders.co.jp/ipo_info/schedule/schedule.asp"
        dfs = common.read_html2(common.Chorme_get(UURL), 0)  # header=0,skiprows=0(省略可能)

        # テーブル番号検索
        num = 0
        for ii in range(len(dfs)):
            if dfs[ii].columns[0] == "上場日":
                num = ii
                break
        today = str(datetime.datetime.now().month) + "/" + str(datetime.datetime.now().day)
        # 新規追加確認
        for idx, row in dfs[num].iterrows():
            dict_w = {}
            if idx % 2 == 0:
                # 上場日確認
                if row[0] == today:
                    for ii in range(len(row)):
                        dict_w[dfs[num].columns[ii]] = row[ii]
                    dict_w = common.add_dict(dict_w['コード'], dict_w)
                    common.insertDB3(DB, table_name, dict_w)

    def Monthly_check(self):
        T_DAY = common.env_time()[0][0:8]
        for t in range(500, 610):
            print(t)
            # 100位まで実行するので1,2
            for i in [1, 2]:
                table_name = "Month_" + str(t)
                UURL = "https://www.stockboard.jp/flash/sel/?sel=sel" + str(t) + "&tech=&st=&ud=&page=" + str(i)
                # header=0,skiprows=0(省略可能)
                dfs = common.read_html2(UURL, 0)
                if type(dfs) == int:
                    break
                # 新規追加確認
                for idx, row in dfs[1].iterrows():
                    if idx < 2:
                        continue

                    dict_w = {}
                    for ii in range(len(row)):
                        dict_w[dfs[1].columns[ii]] = row[ii]
                    dict_w = common.add_dict(dict_w['ｺｰﾄﾞ'], dict_w)
                    dict_w['順位'] = int(dict_w['順位'])
                    dict_w['ｺｰﾄﾞ'] = int(dict_w['ｺｰﾄﾞ'])
                    dict_w['memo'] = T_DAY
                    MON_DB = common.save_path('I03_Monthly.sqlite')
                    common.insertDB3(MON_DB, table_name, dict_w)

    def stock_buy(self):  # 決算発表直
        table_name = "Stock_buy"
        UURL = "https://www.toushi-radar.co.jp/jisya"
        dfs = common.read_html2(UURL, 0)  # header=0,skiprows=0(省略可能)
        # テーブル番号検索
        num = 0

        # 新規追加確認
        for idx, row in dfs[num].iterrows():
            # 重複チェック
            sqls = "select 発表日,コード,rowid from %(table)s where コード = '%(key1)s' and 発表日 == '%(key2)s'" % {'table': table_name, 'key1': row['コード'], 'key2': row['発表日']}
            sql_pd = common.select_sql(DB, sqls)
            if len(sql_pd) > 0:
                continue
            else:
                dict_w = {}
                for ii in range(len(row)):
                    dict_w[dfs[num].columns[ii]] = row[ii]
                dict_w = common.add_dict(dict_w['コード'], dict_w)
                common.insertDB3(DB, table_name, dict_w)

    def split_check(self):
        today = datetime.datetime.now()
        work_day = today.strftime("%m/%d")
        for table_name in ["event_split","event_union"]:
            sqls = "select *,rowid from %(table)s where 権利取最終日 <= '%(key1)s' and 効力発生日 >= '%(key1)s' " % {
                'table': table_name, 'key1': work_day}
            sql_pd = common.select_sql(DB, sqls)
            for i, row in sql_pd.iterrows():
                code = str(row['コード'])
                flag_file = common.save_path(common.RUBY_DATA, "jp")
                flag_file = common.save_path(flag_file, code) + ".txt"
                if os.path.exists(flag_file):
                    print(flag_file)
                    os.remove(flag_file)
                    self.send_msg += code + "_分割銘柄削除RUBY" + "\n"

    def split_check_w(self): #乖離が大きい場合削除
        cnt = 0
        file_dir = common.save_path(common.RUBY_DATA, "jp")
        files = glob.glob(common.save_path(file_dir, "*.txt"))  # ワイルドカードが使用可能
        for code_text in files:
            dir, code = os.path.split(code_text)
            df = pd.DataFrame(index=pd.date_range('2007/01/01', common.env_time()[1][0:10]))
            df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding=common.check_encoding(code_text), header=None))
            df = df.dropna()
            df.columns =['O','H','L','C','V','C2','SS']
            try:
                df['O1']=abs(df['O'] / df['O'].shift(1)-1)
            except:
                os.remove(code_text)
                continue
            days = common.date_diff(str(df.index.max()))
            if df['O1'].max() > 0.4 or days < -5:
                cnt += 1
                os.remove(code_text)
                self.send_msg += code + "_" + str(df['O1'].max()) + "\n"

    def yorituki_hist(self):#後で削除
        code_text = common.save_path(common.RUBY_DATA, "jp")
        code_text = common.save_path(code_text, str(1311)) + ".txt"
        df = pd.DataFrame(index=pd.date_range('2016/01/01', common.env_time()[1][0:10]))
        df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
        df = df.dropna()

        table_name = "yorituki_Hist"
        url = "https://kabuoji3.com/orderbook/"
        ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) '\
            'AppleWebKit/537.36 (KHTML, like Gecko) '\
            'Chrome/55.0.2883.95 Safari/537.36 '
        s = requests.session()
        s.headers.update({'User-Agent': ua})
        for idx in range(len(df)):
            UURL = url + str(df.index[idx])[:10] + "/"
            #リアルタイム処理
            req = s.get(UURL)
            print(UURL)
            soup = BeautifulSoup(req.text, "html.parser")
            stocktable = soup.findAll('div', {'class': 'data_contents'})
            if len(stocktable) < 1:
                continue
            for title in ['寄前気配上げ', '寄前気配下げ']:
                if title == '寄前気配上げ':
                    code_list = stocktable[0].findAll('a')
                else:#'寄前気配下げ'
                    code_list = stocktable[1].findAll('a')
                if len(code_list) < 1:
                    continue
                for i in range(0,len(code_list),2):
                    print(code_list[i].text)
                    dict_w = {}
                    dict_w['コード'] = code_list[i].text
                    dict_w['タイトル'] = title
                    dict_w['日付'] = str(df.index[idx])[:10].replace("-","/")
                    dict_w = common.add_dict(dict_w['コード'], dict_w)
                    common.insertDB3(DB, table_name, dict_w)

    def yorituki_hist_update(self): #後で削除
        table_name = "yorituki_Hist"
        sqls = "select コード,日付,rowid from %(table)s" % {'table': table_name}
        sql_pd = common.select_sql(DB, sqls)
        for i, row in sql_pd.iterrows():
            common.to_number(row)
            code = row['コード']
            code_text = common.save_path(common.RUBY_DATA, "jp")
            code_text = common.save_path(code_text, str(code)) + ".txt"
            if os.path.exists(code_text):
                df = pd.DataFrame(index=pd.date_range('2014/12/01',  str(row['日付'])[:10]))
                df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
                df.columns = ['O', 'H', 'L', 'C', 'V', 'C2', 'SS']
                df = df.dropna()
                df = compf.add_avg_rng(df, 'C', 'L', 'H')
                if len(df) < 250:
                    continue

                if len(df) > 1:
                    print(df.columns)
                    dict_w = {}
                    dict_w['前日日時'] = str(df.index[-2])[:10]
                    dict_w['前日始値'] = df.O[-2]
                    dict_w['前日終値'] = df.C[-2]
                    dict_w['当日日時1'] = str(df.index[-1])[:10]
                    dict_w['当日始値1'] = df.O[-1]
                    dict_w['当日終値1'] = df.C[-1]

                    dict_w['rng7'] = df.rng7[-2]
                    dict_w['rng30'] = df.rng30[-2]
                    dict_w['rng200'] = df.rng200[-2]
                    dict_w['avg7'] = df.avg7[-2]
                    dict_w['avg30'] = df.avg30[-2]
                    dict_w['avg200'] = df.avg200[-2]

                    common.create_update_sql(DB, dict_w, table_name, row['rowid'])

    def update_goba(self):
        for ii in range(100):
            sqls = "select *,rowid from smbc_yorituki_go where now like '%(key1)s%%' and 後場価格 IS NULL" % {'key1': common.env_time()[1][:10]}
            sql_pd = common.select_sql(DB, sqls)
            if len(sql_pd) > 0:
                for i, row in sql_pd.iterrows():
                    code = row['コード']
                    dict_w = {}
                    try:
                        last_price = row['前場終値'].replace(',', '')
                    except:
                        last_price = row['前場終値']
                    yahoo = common.real_stock2(code)
                    try:
                        dict_w['後場価格'] = int(yahoo['price'])
                        dict_w['更新日時'] = common.env_time()[1]
                        print("後場価格チェック",code,int(last_price) , dict_w['後場価格'])
                        if int(last_price) != dict_w['後場価格']:
                            common.create_update_sql(DB, dict_w, 'smbc_yorituki_go', row['rowid'])  #最後の引数を削除すると自動的に最後の行
                    except:
                        continue
                sleep(1)
            else:
                break

    def gyakuhibu(self):
        table_name = "Ghibu"
        url = "https://kabuoji3.com/negative/"
        dfs = common.read_html2(url, 0)  # header=0,skiprows=0(省略可能)

        for idx, row in dfs[0].iterrows():
            dict_w = {}
            dict_w['コード'] = row['コード・名称'][:4]
            dict_w.update(dict(row))
            dict_w['年利'] =  dict_w['年利'].replace('%','')
            dict_w = common.add_dict(dict_w['コード'], dict_w)
            common.insertDB3(DB, table_name, dict_w)

    def bybyhist_hist_update(self):
        table_name = "bybyhist"
        DB_crent = os.path.join(common.DROP_DIR_DB, 'I02_event.sqlite')
        DB = os.path.join(common.DROP_DIR_DB, 'I02_event_Hist.sqlite')
        shutil.copyfile(DB_crent, DB)
        # 全テーブル情報取得
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(DB, sqls)
        for t, rrow in sql_pd.iterrows():
            table_name = rrow['name']
            if table_name == 'MARKET_EXIT':#除外テーブル
                continue
            try:
                sqls = "select AVG20出来高300,当日終値,貸借区分,コード,substr(now,1,10) as day,rowid from %(table)s" % {'table': table_name}
                sql_pd = common.select_sql(DB, sqls)
            except:
                print("NG",table_name)
                continue
            for i, row in sql_pd.iterrows():
                common.to_number(row)
                code = row['コード']
                code_text = common.save_path(common.RUBY_DATA, "jp")
                code_text = common.save_path(code_text, str(code)) + ".txt"
                try:
                    #買い条件に合わないものは削除
                    if table_name == 'ipo':
                        pass
                    elif row['AVG20出来高300'] < 30 or row['当日終値'] < 150 or row['貸借区分'] != 1:
                        sqls = "delete from %(table)s where rowid = '%(key1)s'" % {'table': table_name, 'key1': row['rowid']}
                        common.sql_exec(DB, sqls)
                        continue
                except:
                    print(table_name,code,row['AVG20出来高300'],row['当日終値'])
                    pass

                if os.path.exists(code_text):
                    print(table_name,code,row['day'])
                    last = (datetime.datetime.strptime(row['day'], '%Y/%m/%d') + relativedelta(days=35)).strftime("%Y/%m/%d")
                    df = pd.DataFrame(index=pd.date_range(row['day'], last))
                    df = df.join(pd.read_csv(code_text,index_col=0, parse_dates=True, encoding="cp932", header=None))
                    df.columns = ['O', 'H', 'L', 'C', 'V', 'C2', 'SS']
                    df = df.dropna()
                    #初回のカラム追加
                    if i == 0:
                        dict_w = {}
                        for iii in range(21):
                            dict_w = {}
                            dict_w['DAY' + str(iii)] = None
                            sqls = common.create_update_sql(DB, dict_w, table_name, row['rowid'])
                    if len(df) > 1:
                        dict_w = {}
                        for ii in range(len(df) - 1):
                            if ii > 21:
                                break
                            dict_w['DAY' + str(ii)] = int((df['C'][ii+1] / df['C'][0] -1)*1000000)
                        dict_w['DAY_start'] = str(df.index[0])[:10]
                        dict_w['DAY_end'] = str(df.index[ii])[:10]
                        sqls = common.create_update_sql(DB, dict_w, table_name, row['rowid'])
            sqls = "select '%(table)s' as table_name ,COUNT(*) as COUNT ,round(AVG(DAY0)) as AVG0,round(AVG(DAY1)) as AVG1,round(AVG(DAY2)) as AVG2,round(AVG(DAY3)) as AVG3,round(AVG(DAY4)) as AVG4,round(AVG(DAY5)) as AVG5,round(AVG(DAY6)) as AVG6,round(AVG(DAY7)) as AVG7,round(AVG(DAY8)) as AVG8,round(AVG(DAY9)) as AVG9,round(AVG(DAY10)) as AVG10,round(AVG(DAY11)) as AVG11,round(AVG(DAY12)) as AVG12,round(AVG(DAY13)) as AVG13,round(AVG(DAY14)) as AVG14,round(AVG(DAY15)) as AVG15,round(AVG(DAY16)) as AVG16,round(AVG(DAY17)) as AVG17,round(AVG(DAY18)) as AVG18,round(AVG(DAY19)) as AVG19,round(AVG(DAY20)) as AVG20 from %(table)s" % {'table': table_name}
            sql_pd = common.select_sql(DB, sqls)
            for i, row in sql_pd.iterrows():
                common.insertDB3(DB, 'reports', row.to_dict())

    def all_columns(self):
        DB = os.path.join(common.DROP_DIR_DB, 'I02_event.sqlite')
        # 全テーブル情報取得
        sqls = "select name from sqlite_master where type='table'"
        sql_pd = common.select_sql(DB, sqls)
        for t, rrow in sql_pd.iterrows():
            table_name = rrow['name']
            sqls = "select * from %(table)s where rowid in(select max(rowid) from %(table)s)" % {'table': table_name}
            sql_pd = common.select_sql(DB, sqls)
            if i in list(sql_pd.columns):
                print(i)


    def all_table_now(self):
        files = os.listdir(common.DB_DIR)
        for file in files:
            if file.count(".sqlite") and file.count("-journal") == False:
                print(file)
            else:
                continue
    #        DB = os.path.join(common.DROP_DIR_DB, 'I02_event.sqlite')
            DB = os.path.join(common.DB_DIR, file)

            # 全テーブル情報取得
            sqls = "select name from sqlite_master where type='table'"
            sql_pd = common.select_sql(DB, sqls)
            for t, rrow in sql_pd.iterrows():
                table_name = rrow['name']
                sqls = "select *,rowid from %(table)s" % {'table': table_name}
                sql_pd2 = common.select_sql(DB, sqls)
                for tt, row in sql_pd2.iterrows():
                    dict_w = {}
                    for t_row in ['now', 'uptime']:
                        try:
                            if row[t_row] is None or row[t_row] == "":
                                continue
                        except:
                            continue
                        print(DB,table_name,row['rowid'],t_row)
                        if len(row[t_row]) >= 19:
                            continue
                        sp_work = row[t_row].split(" ")
                        if len(sp_work[0]) >= 10:
                            continue

                        sp_work2 = sp_work[0].split("/")

                        if len(sp_work2[1]) == 1:
                            sp_work2[1] = "0" + sp_work2[1]
                        if len(sp_work2[2]) == 1:
                            sp_work2[2] = "0" + sp_work2[2]
                        if len(sp_work) == 1:
                            dict_w[t_row] = sp_work2[0] + "/" + sp_work2[1] + "/" + sp_work2[2] + " 00:00"
                        else:
                            dict_w[t_row] = sp_work2[0] + "/" + sp_work2[1] + "/" + sp_work2[2] + " " + sp_work[1]

                        sqls = "UPDATE %(table)s SET %(key3)s = '%(key1)s' where rowid = '%(key2)s'" % {'table': table_name, 'key1': dict_w[t_row], 'key2': row['rowid'], 'key3': t_row}
                        print(sqls)
                        common.sql_exec(DB, sqls)

if __name__ == '__main__':
    info = k52_event_rank(0)
    """
    if (len(sys.argv) > 1) and (sys.argv[1] == "debug"):
        import ptvsd
        print("waiting...")
        ptvsd.enable_attach(address=('0.0.0.0', 5678))
        ptvsd.wait_for_attach()
        info.adr_night()
    """
    argvs = sys.argv
    if "morning" == argvs[1]:  # 800
        info.pts_night()
        try:
            msg = "adr_night"
            info.adr_night() #
            msg = "kessan_last"
            info.kessan_last()#決算発表直
            msg = "ipo_chek"
            info.ipo_chek()  #
            msg = "event_check"
            bybypara = {'code': -13, 'comment': '信用取引リスト'}
            result, msg, browser_smbc = k93_smbc.smbc_main(bybypara)
#            info.event_check()#"株式交換", "第三者割当増資", "公募・売出", "銘柄異動", "立会外分売", "社名変更", "株式分割", "株式併合", "業績修正"
        except:
            info.send_msg += "tradersエラー" + __file__ + "\n" + msg
        print(msg)
        info.stock_buy()
        info.gyakuhibu()
        result, msg, browser_smbc = k93_smbc.smbc_main({'code': -12, 'comment': '逆日歩予測', 'table': 'smbc_predict_gyakuhibu'})

        info.event_check()#"株式交換", "第三者割当増資", "公募・売出", "銘柄異動", "立会外分売", "社名変更", "株式分割", "株式併合", "業績修正"
        if info.send_msg != "":
            common.mail_send(u'朝日次イベント完了', info.send_msg)
    elif "gobayori_check" == argvs[1]:  # 1230
        bybypara = {'code': -10,'comment': '寄前気配値情報取得','comment2': '（後場）','table': 'smbc_yorituki_go'}
        result, msg, browser_smbc = k93_smbc.smbc_main(bybypara)
        info.update_goba()
        #逆日歩予報の概要当日 12:40頃
        i = 0
        while i < 1241:
            t = datetime.datetime.now()
            i = int(t.strftime("%H%M"))
            time.sleep(60)
        print("逆日歩予測",i)
        result, msg, browser_smbc = k93_smbc.smbc_main({'code': -12, 'comment': '逆日歩予測', 'table': 'smbc_predict_gyakuhibu'})


    elif "split_check" == argvs[1]:  # 1600
        info.split_check()

    elif "weekly" == argvs[1]:  # 1600
        info.split_check_w()

    elif "night" == argvs[1]:  # 1900
        bybypara = {'code': -11,'comment': 'SMBC値情報取得'}
        aa, bb ,cc = k93_smbc.smbc_main(bybypara)
        info.kabumap_daily()
        info.minkabu_daily()
        info.yahoo_daily()
        info.event_koukai()
        info.market_exit_()
        info.after_update()  # ２回実行する場合は注意

        # 週末のみ
        if common.week_end_day() == 1:
            info.main_weekly()
        common.mail_send(u'夜日次イベント完了', info.send_msg)
    elif "friday" == argvs[1] and common.week_end_day() == 1:  # 1900
        info.bybyhist_hist_update() #イベントへ実績データ追加
    elif "monthly" == argvs[1]:  # 2200
        info.Monthly_check()

    else:
        info.send_msg = "引数が存在しません。:" + argvs[1]
