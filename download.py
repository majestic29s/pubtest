import urllib.request
import sys
import datetime
import os
import requests
from bs4 import BeautifulSoup
import shutil
import filecmp
import time

import common
sys.path.append(common.LIB_DIR)


class download(object):
    def __init__(self):
        self.t = datetime.datetime.now()
        self.time_H = int(self.t.strftime("%H"))
        self.send_msg = ""

        if self.time_H > 16:
            self.date1 = common.env_time()[0][0:8]
            self.date2 = common.env_time()[1][0:10]
            self.date3 = common.env_time()[1][0:10].replace("/", "-")
        else:
            self.date1 = common.last_day().replace("/", "")
            self.date2 = common.last_day()
            self.date3 = common.last_day().replace("/", "-")

    def file_dawnload(self, url, file_path, stop_flag=99):
        try:
            if os.path.exists(file_path) == False:
                urllib.request.urlretrieve(url, "{0}".format(file_path))
                self.send_msg += os.path.basename(file_path) + "をダウンロードしました。" + "\n"
            else:
                temp = file_path + ".tmp"
                urllib.request.urlretrieve(url, "{0}".format(temp))
                if filecmp.cmp(file_path, temp):
                    pass
#                    self.send_msg += file_path + "は更新されてません。" + "\n"
                else:
                    os.remove(file_path)
                    shutil.copyfile(temp, file_path)
                    if stop_flag == 99:
                        name, ext = os.path.splitext(file_path)
                        date_file = name + "_" + self.date1 + ext
                        shutil.copyfile(temp, date_file)
                    self.send_msg += os.path.basename(file_path) + "が更新されました。" + "\n"
                os.remove(temp)
        except:
            self.send_msg += "ダウンロード失敗:" + url + "\n"
            return -1
        return 0

    def setup_basic_auth(self, base_uri, user, password):
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(realm=None, uri=base_uri, user=user, passwd=password)
        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(auth_handler)
        urllib.request.install_opener(opener)

    def daily_m(self):
        # 8商品先物
        url = "http://www.tocom.or.jp/data/tick/TOCOMprice_" + self.date1 + ".csv"
        dir_list = ["99_dict", "D_S_01_TOCOM"]
        file_path = common.save_path(common.dir_join(dir_list), "TOCOMprice.csv")
        self.file_dawnload(url, file_path)

        # USD取引動向
        url = "https://www.usda.gov/oce/commodity/wasde/latest.xls"
        dir_list = ["99_dict", "M_U_01_usda"]
        file_path = common.save_path(common.dir_join(dir_list), "latest.xls")
        self.file_dawnload(url, file_path)
        # CFTC業者別レポート
        for classname in ["deacmelf", "ag_lf", "petroleum_lf"]:
            url = "https://www.cftc.gov/dea/futures/" + classname + ".htm"
            dir_list = ["99_dict", "W_U_02_cftc"]
            file_path = common.save_path(common.dir_join(dir_list), classname + ".txt")
            self.file_dawnload2(url, file_path)

    def daily_m_fx(self):
        # 6為替証拠金取引
        date1 = common.last_day().replace("/", "")
        file_name = "PRT-010-CSV-003-" + date1 + ".CSV"
        url = "https://www.tfx.co.jp/kawase/document/" + file_name
        dir_list = ["99_dict", "D_F_01_syouko"]
        file_path = common.save_path(common.dir_join(dir_list), "PRT-010-CSV-003-today.CSV")
        self.file_dawnload(url, file_path)

        # 7株365証拠金取引
        file_name = "PRT-010-CSV-015-" + date1 + ".CSV"
        url = "https://www.tfx.co.jp/kawase/document/" + file_name
        file_path = common.save_path(common.dir_join(dir_list), "PRT-010-CSV-015-today.CSV")
        self.file_dawnload(url, file_path)

    def taisyaku(self):
        # 貸借区分ファイルダウンロード
        url = "http://www.taisyaku.jp/sys-list/data/other.xlsx"
        dir_list = ["99_dict", "D_K_02_other"]
        file_path = common.save_path(common.dir_join(dir_list), "other.xlsx")
        self.file_dawnload(url, file_path)

        base_url = "http://www.taisyaku.jp/search/result/index/1/"
        ret = requests.get(base_url)
        soup = BeautifulSoup(ret.content, "lxml")
        stocktable = soup.find('div', {'class': 'left-box'})
        url = stocktable.a.get("href")

        dir_list = ["99_dict", "D_K_01_shin"]
        file_path = common.save_path(common.dir_join(dir_list), "shina.csv")
        self.file_dawnload(url, file_path)

        url = "http://www.taisyaku.jp/sys-list/data/seigenichiran.xls"  # 調査する
        dir_list = ["99_dict", "D_K_03_data"]
        file_path = common.save_path(common.dir_join(dir_list), "seigenichiran.xls")
        self.file_dawnload(url, file_path)

    def file_dawnload2(self, url, file_path, stop_flag=99):
#        try:
        ret = requests.get(url)
        if os.path.exists(file_path) == False:
            with open(file_path, 'w') as f: #cp932
                f.write(str(ret.content))
            self.send_msg += os.path.basename(file_path) + "をダウンロードしました。" + "\n"
        else:
            temp = file_path + ".tmp"
            with open(temp, 'w') as f: #utf-8
                f.write(str(ret.content))

            if filecmp.cmp(file_path, temp):
                pass
#                    self.send_msg += file_path + "は更新されてません。" + "\n"
            else:
                os.remove(file_path)
                shutil.copyfile(temp, file_path)
                if stop_flag == 99:
                    name, ext = os.path.splitext(file_path)
                    date_file = name + "_" + self.date1 + ext
                    shutil.copyfile(temp, date_file)
                self.send_msg += os.path.basename(file_path) + "が更新されました。" + "\n"
            os.remove(temp)
#        except:
#            self.send_msg += "ダウンロード失敗:" + url + "\n"
#            return -1
        return 0

if __name__ == '__main__':
    # 昨日YYMMDD
    info = download()
    if info.time_H < 10:
        info.daily_m()

    if info.time_H > 17:
        info.daily_m_fx()

#    info.daily() #有償の為、停止
    info.taisyaku()  # 信用区分 申込停止
    # メール送信
    if info.send_msg.count("ダウンロード失敗"):
        common.mail_send(u'ダウンロードINFO', info.send_msg)
    print("end", __file__)
