import time
import sqlite3
import zipfile
import os
import re
import datetime
from os import listdir
from os.path import isfile, join
import threading
import json
import sys
'''version 1.0.1'''

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)


def zipdir(path, ziph):
# ziph is zipfile handle
    for root, dirs, files in os.walk(path):
        for file in files:
            ziph.write(os.path.join(root, file))

class Log_Checker(threading.Thread):
    def __init__(self, logs_path, tmp_path, db_name):
        self.path = logs_path
        self.tmp_path = tmp_path
        self.db_name = db_name
        #self.archive_path = archive_path
        threading.Thread.__init__(self)

    def run (self):
        db = sqlite3.connect(self.db_name)
        cur = db.cursor()
        #cur.execute('''DROP TABLE IF EXISTS XML''')
        #cur.execute('''DROP TABLE IF EXISTS TMS_LOG''')
        cur.execute(
            '''CREATE TABLE XML ('key' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, file_name TEXT, status TEXT, time TEXT, error_text TEXT)''')
        cur.execute(
            '''CREATE TABLE TMS_LOG ('key' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, first_log_string INTEGER, last_log_string INTEGER, status TEXT)''')
        cur.close()
        db.close()
        x = 1
        while x == 1:
            #time in seconds
            time.sleep(1)
            ready_files = set([f for f in listdir(self.path) if isfile(join(self.path, f))])
            db = sqlite3.connect(self.db_name)
            cur = db.cursor()
            cur.execute('''SELECT file_name FROM XML''')
            requested_files_in_db = cur.fetchall()
            files_in_db = []
            for file in requested_files_in_db:
                files_in_db.append(file[0])
            result = set(ready_files - set(files_in_db))
            cur.close()

            errors = 0
            warnings = 0
            ok = 0
            for file in result:
            #for file in ready_files:
                if file.find("_out") != -1:
                    with open(self.path + "\\" + file, 'r', encoding="utf-8") as opened:
                        data_out = opened.read()
                        now = datetime.datetime.now()
                        if not 'Status="Ok"' in data_out:
                            result_mid = re.search('ErrorText="(.*?)"', data_out)
                            result_final = result_mid.group(0)
                            error_text = result_final.split('"')[1].split('"')[-1]
                            cur = db.cursor()
                            cur.execute('''INSERT INTO XML (file_name, status, time, error_text) VALUES (?, ?, ?, ?)''',
                            (file, "error", datetime.datetime.now(), error_text))
                            zipf = zipfile.ZipFile(str(now.strftime("%Y-%m-%d %H-%M-%S")) + '_' + self.db_name + ".zip", 'w', zipfile.ZIP_DEFLATED)
                            zipdir (self.tmp_path, zipf)
                            zipf.close()
                            errors += 1
                            cur.close()
                        else:
                            cur = db.cursor()
                            cur.execute('''INSERT INTO XML (file_name, status) VALUES (?, ?)''', (file, "ok"))
                            ok += 1
                            cur.close()
                elif file.find("_in") != -1:
                    with open(self.path + "\\" + file, 'r', encoding="utf-8") as opened:
                        data_in = opened.read()
                        now = datetime.datetime.now()
                        if 'error' in data_in:
                            cur = db.cursor()
                            cur.execute('''INSERT INTO XML (file_name, status, time) VALUES (?, ?, ?)''', (file, "error", datetime.datetime.now()))
                            zipf = zipfile.ZipFile(str(now.strftime("%Y-%m-%d %H-%M-%S")) + '_' + self.db_name + ".zip", 'w', zipfile.ZIP_DEFLATED)
                            zipdir (self.tmp_path, zipf)
                            zipf.close()
                            errors += 1
                            cur.close()
                        elif 'warning' in data_in:
                            cur = db.cursor()
                            warnings += 1
                            cur.execute('''INSERT INTO XML (file_name, status) VALUES (?, ?)''', (file, "warning"))
                            zipf = zipfile.ZipFile(str(now.strftime("%Y-%m-%d %H-%M-%S")) + '_' + self.db_name + ".zip", 'w', zipfile.ZIP_DEFLATED)
                            zipdir(self.tmp_path, zipf)
                            zipf.close()
                            cur.close()
                        else:
                            cur = db.cursor()
                            cur.execute('''INSERT INTO XML (file_name, status) VALUES (?, ?)''', (file, "ok"))
                            ok += 1
                            cur.close()

            db.commit()
            db.close()
            print("ok", ok)
            print("warnings", warnings)
            print("errors", errors)

#Log_Checker('d:\\RK7\\XML_LOGS\\1','d:\\RK7\\R Keeper 7\\TMS\\TMP', 'files_1.db', 'd:\RK7\R Keeper 7\CATCHER\TMS_1').start()
#Log_Checker('d:\\RK7\\XML_LOGS\\2','d:\\RK7\\R Keeper 7\\TMS_2\\TMP', 'files_2.db', 'd:\RK7\R Keeper 7\CATCHER\TMS_2').start()
#Log_Checker('d:\\RK7\\XML_LOGS\\3','d:\\RK7\\R Keeper 7\\TMS_3\\TMP', 'files_3.db', 'd:\RK7\R Keeper 7\CATCHER\TMS_3').start()
#Log_Checker('d:\\RK7\\XML_LOGS\\4','d:\\RK7\\R Keeper 7\\TMS_4\\TMP', 'files_4.db', 'd:\RK7\R Keeper 7\CATCHER\TMS_4').start()

filename = 'paths.json'

logs_path, tmp_path, db_name = '', '', ''

with open(filename, 'r') as f_obj:
    file_import = json.load(f_obj)
    logs_path_2 =  file_import[0]
    tmp_path_2 = file_import[1]
    db_name_2 = file_import[2]

Log_Checker(logs_path_2, tmp_path_2, db_name_2).start()
