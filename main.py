import functools
import json
import logging
import pyodbc
import xlrd
import time

from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
from multiprocessing import Pool
from os import listdir
from os.path import isdir, join as pjoin
from sys import exc_info

indir = 'input'


def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()
        obj = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        m, s = divmod(run_time, 60)

        msg = f"\n{obj.file_name}({obj.db_name}.{obj.table_name}) -> " \
              f"execute time: {int(m)} min, {s:.1f} sec, " \
              f"successed records: {obj.info['ok']}, " \
              f"failed records: {obj.info['error']}"

        if obj.unicode_error_flag:
            msg += f" , retry data records: {obj.unicode_error_count}, " \
                   f"retry successed records: {obj.info['retry']}"

        print(msg)
        obj.log.info(msg)

    return wrapper_timer


def read_excel(path):
    print(f"read file '{path}' ...")
    _list = []
    wb = xlrd.open_workbook(path)
    sheet = wb.sheet_by_index(0)

    for i in range(1, sheet.nrows):
        _list.append(sheet.row_values(i))

    wb.release_resources()
    return _list


class SimpleLog(object):
    def __init__(self, path, file_name, console=False):
        self.logger = logging.getLogger(file_name)
        self.logger.setLevel(20)
        fmt = '[%(asctime)s] - %(levelname)s : %(message)s'
        formatter = logging.Formatter(fmt)
        if console:
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            self.__msg = ''
            self.logger.addHandler(stream_handler)

        log_file = pjoin(path, '{}.log'.format(file_name))
        file_handler = logging.FileHandler(log_file, encoding='utf8')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

    def debug(self, msg):
        self.logger.debug(msg)

    def info(self, *msg):
        message = ' '.join([str(i) for i in msg])
        self.logger.info(message)

    def warning(self, *msg):
        message = ' '.join([str(i) for i in msg])
        self.logger.warning(message)

    def error(self, *msg):
        message = ' '.join([str(i) for i in msg])
        self.__msg = message
        self.logger.error(message)

    def critical(self, msg):
        self.logger.critical(msg)

    def log(self, level, msg):
        self.logger.log(level, msg)

    def set_level(self, level):
        self.logger.setLevel(level)

    @property
    def msg(self):
        return self.__msg

    @staticmethod
    def disable():
        logging.disable(50)


class DatabaseHelper(object):
    log = SimpleLog('log', 'informations')
    err_log = SimpleLog('log', 'errors')

    def __init__(self, file_name, db_name='master'):
        self.file_name = None
        self.unicode_error_count = 0
        self._db_name = db_name
        self.file = pjoin(indir, file_name)
        self._table_name = None
        self.flag = False
        self.unicode_error_flag = False
        self.total = 0
        self.count = 0
        self.db_fields = []
        self.error_data_list = []
        self.info = {
            'ok': 0,
            'retry': 0,
            'error': 0,
        }

        settings = json.loads(open('settings.json', encoding='utf8').read())['db']
        self.connect_str = conn_str \
            = 'DRIVER={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}' \
            .format(driver='{ODBC Driver 13 for SQL Server}',
                    server=settings['server'],
                    database=db_name,
                    user=settings['username'],
                    password=settings['password'])
        self.db_conn = pyodbc.connect(conn_str)
        self.cur = self.db_conn.cursor()

    @property
    def db_name(self):
        return self._db_name

    @db_name.setter
    def db_name(self, name):
        self._db_name = name

    @property
    def table_name(self):
        return self._table_name

    @table_name.setter
    def table_name(self, name):
        self._table_name = name

    def get_databases_name(self):
        sql_str = \
            """
            SELECT DB_NAME([dbid]) AS databaseName
            FROM [master].[dbo].[sysdatabases]
            ORDER BY DB_NAME([dbid])
            """

        try:
            self.cur.execute(sql_str)
        except:
            info = exc_info()
            self.err_log.error(f"{info[0]}\t{info[1]}")
        else:
            rows = self.cur.fetchall()
            return [row.databaseName for row in rows]

    def create_table(self):
        fields_str = [f"\"{field}\" NVARCHAR(50)" for field in self.db_fields]
        sql_str = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{self.table_name}' and xtype='U') " \
                  f"CREATE TABLE [{self.db_name}].[dbo].[{self.table_name}] " \
                  f"({','.join(fields_str)})"

        print(f"execute sql command: {sql_str}")
        self.cur.execute(sql_str)

    def __unicode_exception_resolve(self, data):
        cleaned_data = [str(i).encode('utf8', errors='replace') for i in data]
        self.error_data_list.append([i.decode('utf8') for i in cleaned_data])
        self.unicode_error_count += 1

    def insert_to_table(self, data):
        part_of_sql1 = [f"[{i}]" for i in self.db_fields]
        part_of_sql2 = ['?' for _ in range(len(self.db_fields))]
        fields_str = f"({','.join(part_of_sql1)})"
        sql_str = f"INSERT INTO [{self.db_name}].[dbo].[{self.table_name}] " \
                  f"{fields_str} " \
                  f"VALUES ({','.join(part_of_sql2)})"

        try:
            self.cur.execute(sql_str, tuple(data))
        except UnicodeEncodeError:
            info = exc_info()
            self.err_log.warning(f"{info[0]}\t{info[1]} ({self.db_name}.{self.table_name})"
                                 f"\n{data} insert failed, will retry latter.")
            self.__unicode_exception_resolve(data)
        except:
            info = exc_info()
            self.err_log.error(f"{info[0]}\t{info[1]} ({self.db_name}.{self.table_name})\n{data}")
            self.info['error'] += 1
        else:
            self.db_conn.commit()
            if not self.unicode_error_flag:
                self.info['ok'] += 1
            else:
                self.info['retry'] += 1

        self.count += 1
        if not self.unicode_error_flag:
            msg = f"\r{self.count} / {self.total} ..."
        else:
            msg = f"\r{self.count} / {self.unicode_error_count} ..."

        print(msg, end='', flush=True)

    @staticmethod
    def read_excel(path):
        print(f"read file '{path}' ...")
        _list = []
        wb = xlrd.open_workbook(path)
        sheet = wb.sheet_by_index(0)

        fields = sheet.row_values(0)
        for i in range(1, sheet.nrows):
            _list.append(sheet.row_values(i))

        wb.release_resources()
        print(f"Reading '{path}' is completed ...")

        _list.append(fields)
        return _list

    @staticmethod
    def read_file_with_processes(file_path):
        with ProcessPoolExecutor(4) as ex:
            data_list = list(ex.map(DatabaseHelper.read_excel, file_path))
        return data_list

    def __handle_task(self, data):
        result_list = []

        for i in data:
            self.insert_to_table(i)

    def read_file(self, file_or_dir) -> list:
        data_list = []
        self.file_name = file_or_dir

        if isdir(file_or_dir):
            file_list = listdir(file_or_dir)
            file_path = [pjoin(file_or_dir, file) for file in file_list]
            l = DatabaseHelper.read_file_with_processes(file_path)

            for file_name, data in zip(file_list, l):
                self.db_fields = data.pop()
                if not self.flag:
                    self.create_table()
                    self.flag = True

                self.total = len(data)
                print(f"start to insert file '{file_name}' ...")
                self.__handle_task(data)
                print(f"\nfinished insert file '{file_name}' ...")
                self.count = 0
            return None

        elif file_or_dir.endswith('.xlsx'):
            data = DatabaseHelper.read_excel(file_or_dir)
            self.db_fields = data.pop()
            self.create_table()
            data_list.extend(data)

        elif file_or_dir.endswith('.txt'):
            print(f"read file '{file_or_dir}' ...")
            with open(file_or_dir, encoding='utf8') as f:
                lines = f.readlines()

                if not self.flag:
                    fields = lines.pop(0).strip().split(',')
                    self.db_fields = [i.replace(' ', '') for i in fields]
                    self.create_table()

                for line in lines:
                    data = line.strip().split(',')
                    clean_data = [i.replace(' ', '').replace('\u3000', '') for i in data]
                    data_list.append(clean_data)

        elif file_or_dir.endswith('.json'):
            print(f"read file '{file_or_dir}' ...")
            with open(file_or_dir, encoding='utf8') as f:
                raw_data_list = json.loads(f.read())
                self.db_fields = list(raw_data_list[0].keys())
                self.create_table()

                for data in raw_data_list:
                    roc = str(int(data['InvYear']) - 1911).rjust(3, '0')
                    data['InvYear'] = roc
                    clean_data = [i.replace(' ', '').replace('\u3000', '') for i in data.values()]
                    data_list.append(clean_data)

        elif file_or_dir.endswith('.utf8') or file_or_dir.endswith('.ucs'):
            print(f"read file '{file_or_dir}' ...")
            unique_dict = {}
            duplicate_set = set()
            with open(file_or_dir, encoding='utf8') as f:
                if not self.flag:
                    self.db_fields = [
                        'header', 'pid', 'name', 'birth', 'householdNumber', 'address',
                        'role', 'annotation', 'emigrationType', 'householdCode'
                    ]
                    self.create_table()
                    self.flag = True

                for index, line in enumerate(f, start=1):
                    l = line.strip().split(',')
                    l = [i.replace(' ', '').replace('\u3000', '') for i in l]
                    l.pop()

                    if len(l) != 13 and len(l) != 14:
                        self.err_log.error(f"Line {index} is invalid (len={len(l)}): {l}")
                        continue

                    if len(l) == 13:
                        for i in range(4):
                            l[4] += l.pop(5)
                        l.insert(2, None)
                    elif len(l) == 14:
                        for i in range(4):
                            l[5] += l.pop(6)

                    pid = l[1]
                    if pid not in unique_dict:
                        unique_dict[pid] = None
                        data_list.append(l)
                    else:
                        duplicate_set.add(pid)

                self.err_log.warning(f"These id are duplicate: {duplicate_set}")

        elif file_or_dir.endswith('.csv'):
            import csv
            print(f"read file '{file_or_dir}' ...")
            with open(file_or_dir, encoding='utf8') as f:
                datas = csv.reader(f.readlines())

                for data in datas:
                    if not self.flag:
                        self.db_fields = data
                        self.create_table()
                        self.flag = True
                    else:
                        data_list.append(data)

        return data_list

    @timer
    def inert_to_database(self):
        data = self.read_file(self.file)

        if data is not None:
            self.total = len(data)
            print(f"start to insert data ...")
            self.__handle_task(data)
            self.count = 0

        if self.error_data_list:
            print('\nstart to insert error data ...')
            self.unicode_error_flag = True
            self.__handle_task(self.error_data_list)

        self.cur.close()
        self.db_conn.close()
        return self


if __name__ == '__main__':
    file_list = listdir(indir)
    new_line = '\n'
    msg = ['[{index}] {f_name}'.format(f_name=el, index=str(i).rjust(2, '0'))
           for i, el in enumerate(file_list)]

    max_len = max([len(i.encode('utf8')) for i in msg])
    display_msg = [j.ljust(max_len, ' ') + '\n'
                   if i % 2 == 0
                   else j.ljust(max_len, ' ')
                   for i, j in enumerate(msg, start=1)]

    file_index = input(
        f"{''.join(display_msg)}{new_line + 'Choicing import file: '}"
    )

    helper = DatabaseHelper(file_list[eval(file_index)])
    dbs_name = helper.get_databases_name()
    max_len = max([len(i) for i in dbs_name])
    display_dbs_name = [f"[{str(i-1).rjust(2, '0')}] {j.ljust(max_len, ' ')}" + '\n'
                        if i % 2 == 0
                        else f"[{str(i-1).rjust(2, '0')}] {j.ljust(max_len, ' ')}" + ' '
                        for i, j in enumerate(dbs_name, start=1)]

    db_index = input('\n' + f"{''.join(display_dbs_name)}{new_line}Choicing database: ")
    helper.db_name = dbs_name[int(db_index)]
    table_name = input('table name: ')
    helper.table_name = table_name

    helper.inert_to_database()
