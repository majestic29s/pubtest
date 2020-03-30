#!/usr/bin/env python
# -*- coding: utf-8 -*-
#%matplotlib inline
import numpy as np
#import statsmodels.api as sm
#import matplotlib.pyplot as plt
import pandas as pd
import pandas.tseries as pdt
from datetime import date
#from pandas.tools.plotting import scatter_matrix
#import seaborn as sns
import common
import datetime
import os,csv
import shutil
import common_profit as compf

class profit:
    def __init__(self,num):
        t = datetime.datetime.now()
        self.date = t.strftime("%Y%m%d%H%M%S")
        #保存フォルダルート
        self.S_DIR = os.path.join(r"C:\data\90_profit\06_output",num,self.date + "_FX")
        #保存フォルダ
        self.INPUT_DIR = r"C:\data\90_profit\05_input\FX"
        os.mkdir(str(self.S_DIR))
        #編集ファイル格納場所（通常は使用しない)
        self.dir = r"C:\data\90_profit\05_input\FX_soruce"
        #本スクリプトコピー
        shutil.copy2(__file__, self.S_DIR)


    #移動平均を基準とした上限・下限のブレイクアウト
    def breakout_ma_std(self,tsd,window,multi,para_val,para_key):
        #ub=moving average + std * multi
        #lb=moving average - std * multi
        #entry: long: s>ub; short: s<lb
        #exit: long:s<ma;  short: s>ma
#        print(tsd)
        m=tsd.Close.rolling(window).mean().dropna()
        m=m.shift(1)
        s=tsd.Close.rolling(window).std()
        s=s.shift(1)
        y=pd.concat([tsd.now,tsd.Close,m,s],axis=1).dropna()
        y = y.set_index('now')
        y.columns=['Close','ma_m','ma_s']
        y['ub']=y.ma_m+y.ma_s*multi
        y['lb']=y.ma_m-y.ma_s*multi
#        y[para_key] = tsd[para_key]

#        BuyExit[N-2] = SellExit[N-2] = True #最後に強制エグジット
#        BuyPrice = SellPrice = 0.0 # 売買価格
#        buy_key = para_key
        #sell_key = 1-para_key
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N)
        n = np.zeros(N)
        #init----------------------------------
        buy=0
        sell=0
        y.to_csv("test.csv")
        for i in range(1100,len(y)):
            da=y.index[i]
            c=y.Close.iloc[i]
            SumPL[i]=SumPL[i-1] #レポート用
#            if c<y.lb.iloc[i] and sell==0 and y[para_val].iloc[i] >= sell_key :#entry short-position
#            if c<y.lb.iloc[i] and sell==0 and y.index[i].hour == para_key :#entry short-position
            if c<y.lb.iloc[i] and sell==0  :#entry short-position
                sell=c
                n[i]=-1
            elif c>y.ma_m.iloc[i] and sell!=0:#exit short-position
                pl[i]=sell-c
                ShortPL[i] = sell-c #レポート用
                sell=0
#            if c>y.ub.iloc[i] and buy==0 and y[para_val].iloc[i] <= buy_key:#entry short-position
#            if c>y.ub.iloc[i] and buy==0 and y.index[i].hour == para_key:#entry short-position
            elif c>y.ub.iloc[i] and buy==0 :#entry short-position
                buy=c
                n[i]=1
            elif c<y.ma_m.iloc[i] and buy!=0:#exit short-position
                pl[i]=c-buy
                LongPL[i] = c-buy #レポート用
                buy=0
            SumPL[i] = SumPL[i] + ShortPL[i] + LongPL[i]  #レポート用
        y['pl'] = pl
        y['n'] = n
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)

    #過去の高値・安値を用いたブレイクアウト戦略
    def breakout_simple(self,tsd,window0,window9,para_val,para_key):
        #ub0=max - n0 days
        #lb0=min - n0 days
        #ub9=max - n9 days
        #lb9=min - n9 days

        y=tsd.dropna()
        y = y.set_index('now')
        y['ub0']=y['Close'].rolling(window0).max().shift(1)
        y['lb0']=y['Close'].rolling(window0).min().shift(1)
        y['ub9']=y['Close'].rolling(window9).max().shift(1)
        y['lb9']=y['Close'].rolling(window9).min().shift(1)

        y=y.dropna()
#        buy_key = para_key
        #sell_key = 1-para_key
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N)
        n = np.zeros(N)

        #init----------------------------------
        buy=0
        sell=0
        for i in range(1100,len(y)):
            da=y.index[i]
            c=y.Close.iloc[i]
            SumPL[i]=SumPL[i-1] #レポート用
#            if c<y.lb0.iloc[i] and sell==0 and y.index[i].hour == para_key:#entry short-position
            if c<y.lb0.iloc[i]<y.lb0.iloc[i-5] and sell==0 :#entry short-position
#            if c<y.lb0.iloc[i] and sell==0 and y[para_val].iloc[i] >= sell_key:#entry short-position
                sell=c
                n[i]=-1
            elif c>y.ub9.iloc[i] and sell!=0:#exit short-position
                pl[i]=sell-c
                ShortPL[i] = sell-c #レポート用
                sell=0
            elif c>y.ub0.iloc[i]>y.ub0.iloc[i-5] and buy==0  :#entry short-position
#            if c>y.ub0.iloc[i] and buy==0 and y.index[i].hour == para_key:#entry short-position
#            if c>y.ub0.iloc[i] and buy==0 and y[para_val].iloc[i] <= buy_key:#entry short-position
                buy=c
                n[i]=1
            elif c<y.lb9.iloc[i] and buy!=0:#exit short-position
                pl[i]=c-buy
                LongPL[i] = c-buy #レポート用
                buy=0
            SumPL[i] = SumPL[i] + ShortPL[i] + LongPL[i]  #レポート用
        y['pl'] = pl
        y['n'] = n
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)

    #フィルター付き高値・安値のブレイクアウト
    def breakout_simple_f(self,tsd,window0,window9,f0,f9,para_val,para_key):
        #ub0=max - n0 days
        #lb0=min - n0 days
        #ub9=max - n9 days
        #lb9=min - n9 days
        #filter long - f0 days
        #filter short - f9 days
        y=tsd.dropna()
        y = y.set_index('now')
        y['ub0']=y['Close'].rolling(window0).max().shift(1)
        y['lb0']=y['Close'].rolling(window0).min().shift(1)
        y['ub9']=y['Close'].rolling(window9).max().shift(1)
        y['lb9']=y['Close'].rolling(window9).min().shift(1)
        y['f0']=y['Close'].rolling(f0).mean().shift(1)
        y['f9']=y['Close'].rolling(f9).mean().shift(1)
        y=y.dropna()
#        buy_key = para_key
        #sell_key = 1-para_key
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N)
        n = np.zeros(N)
        #init----------------------------------
        buy=0
        sell=0
        for i in range(1100,len(y)):
            da=y.index[i]
            c=y.Close.iloc[i]
            SumPL[i]=SumPL[i-1] #レポート用
#            if c<y.lb0.iloc[i] and sell==0 and y.f9.iloc[i]>y.f0.iloc[i] and y.index[i].hour == para_key:#entry short-position
            if c<y.lb0.iloc[i] and sell==0 and y.f9.iloc[i]>y.f0.iloc[i] :#entry short-position
#            if c<y.lb0.iloc[i] and sell==0 and y.f9.iloc[i]>y.f0.iloc[i] and y[para_val].iloc[i] >= sell_key:#entry short-position
                sell=c
                n[i]=-1
            elif c>y.ub9.iloc[i] and sell!=0:#exit short-position
                pl[i]=sell-c
                ShortPL[i] = sell-c #レポート用
                sell=0
#            if c>y.ub0.iloc[i] and buy==0 and y.f9.iloc[i]<y.f0.iloc[i] and y.index[i].hour == para_key:#entry short-position
            elif c>y.ub0.iloc[i] and buy==0 and y.f9.iloc[i]<y.f0.iloc[i] :#entry short-position
#            if c>y.ub0.iloc[i] and buy==0 and y.f9.iloc[i]<y.f0.iloc[i] and y[para_val].iloc[i] <= buy_key:#entry short-position
                buy=c
                n[i]=1
            elif c<y.lb9.iloc[i] and buy!=0:#exit short-position
                pl[i]=c-buy
                LongPL[i] = c-buy #レポート用
                buy=0
            SumPL[i] = SumPL[i] + ShortPL[i] + LongPL[i]  #レポート用
        y['pl'] = pl
        y['n'] = n
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)

    #クロスオーバー移動平均
    def breakout_ma_two(self,tsd,window0,window9,para_val,para_key):
        m0=tsd.Close.rolling(window0).mean().shift(1).dropna()
        m9=tsd.Close.rolling(window9).mean().shift(1).dropna()
#        y=pd.concat([tsd.Close,tsd[para_key],m0,m9],axis=1).dropna()
        y = pd.concat([tsd.now,tsd.Close, m0, m9], axis=1).dropna()
        y = y.set_index('now')

        y.columns=['Close','ma0','ma9']
        y['pl']=0
        y['n']=0
#        buy_key = para_key
        #sell_key = 1-para_key
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N)
        n = np.zeros(N)

        #init----------------------------------
        buy=0
        sell=0
        for i in range(1100,len(y)):
            da=y.index[i]
            c=y.Close.iloc[i]
            m0=y.ma0.iloc[i]
            m9=y.ma9.iloc[i]
            SumPL[i]=SumPL[i-1] #レポート用
            if c<m0<m9 and sell==0 :#entry short-position
#            if m0<m9 and sell==0 and y.index[i].hour == para_key:#entry short-position
#            if m0<m9 and sell==0 and y[para_val].iloc[i] >= sell_key:#entry short-position
                sell=c
                n[i]=-1
            elif m0>m9 and sell!=0:#exit short-position
                pl[i]=sell-c
                ShortPL[i] = sell-c #レポート用
                sell=0
            elif c>m0>m9 and buy==0 :#entry short-position
#            if m0>m9 and buy==0 and y.index[i].hour == para_key:#entry short-position
#            if m0>m9 and buy==0 and y[para_val].iloc[i] <= buy_key:#entry short-position
                buy=c
                n[i]=1
            elif m0<m9 and buy!=0:#exit short-position
                pl[i]=c-buy
                LongPL[i] = c-buy #レポート用
                buy=0
            SumPL[i] = SumPL[i] + ShortPL[i] + LongPL[i]  #レポート用
        y['pl'] = pl
        y['n'] = n
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)

    #3つの移動平均を使った戦略
    def breakout_ma_three(self,tsd,window0,window5,window9,para_val,para_key):
        m0=tsd.Close.ewm(span=window0).mean().shift(1).dropna()
        m5=tsd.Close.ewm(span=window5).mean().shift(1).dropna()
        m9=tsd.Close.ewm(span=window9).mean().shift(1).dropna()
        y = pd.concat([tsd.now,tsd.Close, m0, m5, m9], axis=1).dropna()
        y = y.set_index('now')
        y.columns=['Close','ma0','ma5','ma9']
#        y=pd.concat([tsd.Close,tsd[para_val],m0,m9,m5],axis=1).dropna()
#        y.columns=['Close',para_val,'ma0','ma9','ma5']
        y['pl']=0
        y['n']=0
#        buy_key = para_key
        #sell_key = 1-para_key
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N)
        n = np.zeros(N)

        #init----------------------------------
        buy=0
        sell=0
        for i in range(1100,len(y)):
            da=y.index[i]
            c=y.Close.iloc[i]
            m0=y.ma0.iloc[i]
            m5=y.ma5.iloc[i]
            m9=y.ma9.iloc[i]
            SumPL[i]=SumPL[i-1] #レポート用
#            if sell==0 and m0<m9 and m0<m5 and m9<m5 and y.index[i].hour == para_key:#entry short-position
            if sell==0 and c<m0<m5<m9 :#entry short-position
#            if sell==0 and m0<m9 and m0<m5 and m9<m5 and y[para_val].iloc[i] >= sell_key:#entry short-position
                sell=c
                n[i]=-1
            elif (m0>m9 and sell!=0) or (m0>m5 and sell!=0) :#exit short-position
                pl[i]=sell-c
                ShortPL[i] = sell-c #レポート用
                sell=0
            elif buy==0 and c>m0>m5>m9 :#entry short-position
#            if buy==0 and m0>m9 and m0>m5 and m9>m5 and y.index[i].hour == para_key:#entry short-position
#            if buy==0 and m0>m9 and m0>m5 and m9>m5 and y[para_val].iloc[i] <= buy_key:#entry short-position
                buy=c
                n[i]=1
            elif (m0<m9 and buy!=0) or (m0<m5 and buy!=0) :#exit short-position
                pl[i]=c-buy
                LongPL[i] = c-buy #レポート用
                buy=0
            SumPL[i] = SumPL[i] + ShortPL[i] + LongPL[i]  #レポート用
        y['pl'] = pl
        y['n'] = n
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)

    def breakout_ma_four(self,tsd,window0,window5,window9,window10,para_val,para_key):
        m0=tsd.Close.ewm(span=window0).mean().shift(1).dropna()
        m5=tsd.Close.ewm(span=window5).mean().shift(1).dropna()
        m9=tsd.Close.ewm(span=window9).mean().shift(1).dropna()
        m10=tsd.Close.ewm(span=window10).mean().shift(1).dropna()
        y = pd.concat([tsd.now,tsd.Close, m0, m5, m9, m10], axis=1).dropna()
        y = y.set_index('now')
        y.columns=['Close','ma0','ma5','ma9','ma10']
#        y=pd.concat([tsd.Close,tsd[para_val],m0,m9,m5],axis=1).dropna()
#        y.columns=['Close',para_val,'ma0','ma9','ma5']
        y['pl']=0
        y['n']=0
#        buy_key = para_key
        #sell_key = 1-para_key
        #レポート用
        N = len(y) #FXデータのサイズ
        LongPL = np.zeros(N) # 買いポジションの損益
        ShortPL = np.zeros(N) # 売りポジションの損益
        SumPL = np.zeros(N) # 売りポジションの損益
        pl = np.zeros(N)
        n = np.zeros(N)

        #init----------------------------------
        buy=0
        sell=0
        for i in range(1100,len(y)):
            da=y.index[i]
            c=y.Close.iloc[i]
            m0=y.ma0.iloc[i]
            m5=y.ma5.iloc[i]
            m9=y.ma9.iloc[i]
            m10=y.ma10.iloc[i]
            SumPL[i]=SumPL[i-1] #レポート用
#            if sell==0 and m0<m9 and m0<m5 and m9<m5 and y.index[i].hour == para_key:#entry short-position
            if sell==0 and c<m0<m5<m9<m10:#entry short-position
#            if sell==0 and m0<m9 and m0<m5 and m9<m5 and y[para_val].iloc[i] >= sell_key:#entry short-position
                sell=c
                n[i]=-1
            elif (m5>m9 and sell!=0) or (m0>m5 and sell!=0) :#exit short-position
                pl[i]=sell-c
                ShortPL[i] = sell-c #レポート用
                sell=0
            elif buy==0 and c>m0>m5>m9>m10:#entry short-position
#            if buy==0 and m0>m9 and m0>m5 and m9>m5 and y.index[i].hour == para_key:#entry short-position
#            if buy==0 and m0>m9 and m0>m5 and m9>m5 and y[para_val].iloc[i] <= buy_key:#entry short-position
                buy=c
                n[i]=1
            elif (m5<m9 and buy!=0) or (m0<m5 and buy!=0):#exit short-position
                pl[i]=c-buy
                LongPL[i] = c-buy #レポート用
                buy=0
            SumPL[i] = SumPL[i] + ShortPL[i] + LongPL[i]  #レポート用
        y['pl'] = pl
        y['n'] = n
        return pd.DataFrame({'LongPL':LongPL, 'ShortPL':ShortPL,'Sum':SumPL}, index=y.index)

    def csv_connect(slef,dir,key):
        all = []
        flag = 0
        for root, dirs, files in os.walk(dir):
            for fname in files:
                filename = os.path.join(root, fname)
                if filename.count(key):
#                    print(filename)
#                    temp = pd.read_csv(filename,engine='python',index_col=0,parse_dates=True,encoding=common.check_encoding(filename),skiprows=1).iloc[:,0:4]
                    temp = pd.read_csv(filename,engine='python',index_col=0,parse_dates=True,encoding=common.check_encoding(filename),skiprows=1).iloc[:,0:4]
                    temp.columns = ["Open","High","Low","Close"]
                    temp.index_label='date'
                    if flag == 0:
                        flag = 1
                        all = temp
                    else:
                        all = pd.concat([all,temp])

        return all
    def interval(self,all,priod):
        a = all.resample(priod).first()
        return a
    def save_to_csv(self,save_name,title,backreport):
        #ヘッダー追加
        if os.path.exists(save_name) == False:
            dic_name = ",".join([str(k[0]).replace(",","")  for k in backreport.items()])+"\n"
            with open(save_name, 'w') as f:
                f.write("now,stockname,"+dic_name)
        #1列目からデータ挿入
        dic_val = ",".join([str(round(k[1],3)).replace(",","")  for k in backreport.items()])+"\n"
        with open(save_name, 'a', encoding="utf-8") as f:
            f.write(common.env_time()[1] +"," + title+","+dic_val)
    def priod_edit(self,tsd,priod):
            #       ５分間隔のデータ作成
            o = tsd.Close.resample(priod).first().dropna()
            h = tsd.Close.resample(priod).max().dropna()
            l = tsd.Close.resample(priod).min().dropna()
            c = tsd.Close.resample(priod).last().dropna()
            tsd = pd.concat([o,h,l,c],axis=1)
            tsd.columns = ["Open","High","Low","Close"]
            #乖離平均追加
            for t in [7,30,200,1000]:
#                h=tsd['High'].rolling(t).max().shift(1)
#                l=tsd['Low'].rolling(t).min().shift(1)
#                c=tsd['Close'].shift(1)
                tsd['rng'+str(t)]=round((tsd['Close'].shift(1)-tsd['Low'].rolling(t).min().shift(1))/(tsd['High'].rolling(t).max().shift(1)-tsd['Low'].rolling(t).min().shift(1)),2)
                tsd['avg'+str(t)]=round(tsd['Close'].shift(1)/tsd['Close'].rolling(t).mean().shift(1)-1,2)
            return tsd

    def priod_edit2(self,code):
        if code == 'BTCJPY':
#            tsd_root = pd.read_csv(os.path.join(info.INPUT_DIR, 'bitflyer_BTCJPY.csv'), index_col=0, parse_dates=True)  #●●●●●●●●●●●
            tsd_root = common.select_sql('I05_bitcoin.sqlite', 'select *,rowid from bitflyer_BTCJPY where substr(now,1,10) >= "2018/01/31"')
            c = tsd_root['S3_R'].astype(np.float64)
        elif code in ['topixL','J225L','jpx400','mather']:
            tsd_root = common.select_sql('I08_futures.sqlite', 'select *,rowid from %(table)s' % {'table': code})
            tsd_root = tsd_root[tsd_root['現在値'] != '--']
            c = tsd_root['現在値'].astype(np.float64)
        elif code in ['GOLD','GOMU','para','WTI']:
            tsd_root = common.select_sql('I06_cmd.sqlite', 'select *,rowid from %(table)s' % {'table': code})
            tsd_root = tsd_root[tsd_root['現値'] != '--']
            c = tsd_root['現値'].astype(np.float64)
        else:
#            tsd_root = pd.read_csv(os.path.join(info.INPUT_DIR, 'gmofx.csv'), index_col=0, parse_dates=True)  #●●●●●●●●●●●
            tsd_root = common.select_sql('I07_fx.sqlite', 'select *,rowid from gmofx')
            c = tsd_root[code[:3] + "/" + code[-3:]].astype(np.float64)
        n = tsd_root['now']
        tsd = pd.concat([n, c], axis=1)
        tsd.columns = ['now', 'Close'][:len(tsd.columns)]
#        tsd = tsd.set_index('now')
        #乖離平均追加
        for t in [7,30,200,1000]:
            tsd['rng'+str(t)]=round((tsd['Close'].shift(1)-tsd['Close'].rolling(t).min().shift(1))/(tsd['Close'].rolling(t).max().shift(1)-tsd['Close'].rolling(t).min().shift(1)),2)
            tsd['avg'+str(t)]=round(tsd['Close'].shift(1)/tsd['Close'].rolling(t).mean().shift(1)-1,3)
        return tsd


if __name__ == "__main__":
    t = datetime.datetime.now()
    info = profit('FX')
    ETF = [ 'BTCJPY','GBPJPY','AUDJPY', 'USDJPY', 'EURJPY','AUDUSD', 'GBPUSD', 'EURUSD']
    ETF = ['topixL', 'J225L', 'jpx400', 'mather', 'GOLD', 'GOMU', 'para', 'WTI']

#時間指定ネタ
#    tsd = pd.read_csv( 'USDJPY_15.csv',index_col=0,parse_dates=True)
#    td=tsd[tsd.index.hour==8]


    for code in ETF:
#        try:
#        tsd = info.csv_connect(info.dir + "//" + code ,code)#★★★★★★★
        para = ['avg30', 'avg100', 'avg500', 'avg2000']
#        para =[ i for i in range(24)]
        para = [0]
        col = 0.1
#       CSVから読み込む場合
        tsd = info.priod_edit2(code)
#        tsd = pd.read_csv(os.path.join(info.INPUT_DIR ,'AUDJPY_100_2.5_1BacktestReport.csv'),index_col=0,parse_dates=True)#●●●●●●●●●●●
#        aaa = tsd[(tsd['ShortPL'] != 0.0) | (tsd['LongPL'] != 0.0)]
        if len(tsd) > 1:
    #       ５分間隔のデータ作成
#            tsd = info.priod_edit(tsd,'15T')#★★★★★★★
#            tsd.to_csv(os.path.join(info.INPUT_DIR ,code + '_15.csv') )#★★★★★★★

#                Trade, PL=info.breakout_simple_f(tsd,window0,window9,f0,f9,'rng200',i)
#                Equity,backreport = info.BacktestReport(Trade, PL)
#                info.save_to_csv(save_name,code+ str(i) + "_3breakout_simple_f",backreport)

#                all_ = pd.concat([tsd,Trade,PL],axis=1)
#                all_.to_csv(code + str(i) + '_dital.csv' )

#                print("end")
#                exit()

            for i in range(5, 100, 10):
                for ii in range(10, 300, 30):
                    for iii in range(50, 2000, 100):
                        for iiii in range(500, 5000, 500):

                            window0 = i
                            window5 = ii
                            window9 = iii
                            window100 = iiii
                            if i > ii > iii > iiii or i < ii < iii < iiii:
                                title = code + "_" + str(window0) + "_" + str(window5) + "_" + str(window9) + "_" + str(window100) + "_0_breakout_ma_four.csv"
                                PL = info.breakout_ma_four(tsd, window0, window5, window9,window100, col, window0)
                                Equity, backreport = compf.BacktestReport(PL, title,info.S_DIR)
#            continue
            """
            #追加テスト
            window=350
#            fx = 'AUD/JPY'
#            fx = u'GBP/JPY'
            s_time = 17
            e_time = 20

            window = 100
            multi = 2.5
            title = code + "_" + str(window) + "_" + str(multi) + "_1BacktestReport.csv"
            PL = info.breakout_ma_std(tsd, window, multi, col, 1)
            PL.to_csv(os.path.join(info.S_DIR, title))
            Equity, backreport = compf.BacktestReport(PL, title, info.S_DIR)
            print("End")
            exit(9)

            #追加テスト終了
            """
            print("1")
            for i in range(2,350,50):
#                window = 350
                window = i
                multi = 2.5
                title = code + "_" + str(window) + "_" + str(multi) + "_1_breakout_ma_std.csv"
                PL = info.breakout_ma_std(tsd, window, multi, col, i)
                Equity, backreport = compf.BacktestReport(PL, title,info.S_DIR)

            print("2")
            for i in range(2, 200, 50):
                for ii in range(2, 100, 20):
#                window0 = 20
#                window9 = 10
                    window0 = i
                    window9 = ii
                    if i == ii:
                        continue
                    title = code + "_" + str(window0) + "_" + str(window9) + "_2_breakout_simple.csv"
                    PL = info.breakout_simple(tsd, window0, window9, col, i)
                    Equity, backreport = compf.BacktestReport(PL, title,info.S_DIR)

            print("3")
            for i in range(2, 200, 50):
                for ii in range(2, 100, 20):
                    for iii in range(2, 100, 20):
                        for iiii in range(2, 400, 50):

                            #                window0 = 20
                            #                window9 = 10
                            #                    f0 = 25
                            #                    f9 = 350
                            window0 = i
                            window9 = ii
                            f0 = iii
                            f9 = iiii
                            if i == ii or iii == iiii:
                                continue
                            title = code + "_" + str(window0) + "_" + str(window9) + "_" + str(f0) + "_" + str(f9) + "_3_breakout_simple_f.csv"
                            PL = info.breakout_simple_f(tsd, window0, window9, f0, f9, col, i)
                            Equity, backreport = compf.BacktestReport(PL, title,info.S_DIR)

            print("4")
            for i in range(2, 200, 20):
                for ii in range(2, 400, 50):
                    #                window0 = 100
                    #                window9 = 350
                    window0 = i
                    window9 = ii
                    if i == ii:
                        continue

                    title = code + "_" + str(window0) + "_" + str(window9) + "_4_breakout_ma_two.csv"
                    PL = info.breakout_ma_two(tsd, window0, window9, col, i)
                    Equity, backreport = compf.BacktestReport(PL, title,info.S_DIR)

            print("5")
            for i in range(1, 300, 30):
                for ii in range(50, 2000, 100):
                    for iii in range(300, 3000, 300):
#                window0 = 100
#                window9 = 250
#                window5 = 350
                        window0 = i
                        window5 = ii
                        window9 = iii
                        if i < ii < iii or i > ii > iii:
                            title = code + "_" + str(window0) + "_" + str(window5) + "_" + str(window9) + "_5_breakout_ma_three.csv"
                            PL = info.breakout_ma_three(tsd, window0, window5, window9, col, i)
                            Equity, backreport = compf.BacktestReport(PL, title,info.S_DIR)

    #            詳細調査の場合
    #            all_ = pd.concat([tsd,axis=1)
    #            all_.to_csv(code + 'dital.csv' )



#        except:
#            pass
    sum_time = datetime.datetime.now() - t
    print(t,datetime.datetime.now(),sum_time)
    print("end",__file__)
