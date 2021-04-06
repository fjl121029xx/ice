#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @Time  : 2021/4/2 16:22
__author__ = 'the king of north'

import subprocess
import time

import pymysql
import hashlib
import os


class TimeoutError(Exception):
    def __init__(self, db, tb):
        # 超时会设置3
        xlien = MysqlClient('mysql.yqs.master.hualala.com', 3306, 'root',
                            'MySql!!!123',
                            'iceberg_hbi')
        xlien.execute("update hbi_iceberg_insert set isok =3 where db=\'{db}\' and tb=\'{tb}\'".format(db=db, tb=tb))


class MysqlClient:
    """
    mysql 客户端
    """

    def __init__(self, host, port, user, password, database):
        """
        初始化客户端
        :param mysql_conf: mysql配置
        """

        self._conn = pymysql.connect(host=host,
                                     port=port,
                                     user=user,
                                     password=password,
                                     database=database,
                                     charset='utf8', autocommit=True)
        self.dict = dict

    def query(self, sql, dict_cursor=False):
        """
        查询
        :param sql: sql语句
        :return: 查询结果
        """
        if dict_cursor:
            with self._conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
        else:
            with self._conn.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()

    def execute(self, sql):
        """
        执行非查询sql
        :param sql: sql语句
        """
        with self._conn.cursor() as cursor:
            cursor.execute(sql)

    def close(self):
        self._conn.close()


def execute(cmd):
    with os.popen(cmd) as process:
        output = process.read()
        print("【cmd】---> " + output.strip())
        return output.strip()


#
def run_create():
    xlien = MysqlClient('mysql.yqs.master.hualala.com', 3306, 'root',
                        'MySql!!!123',
                        'iceberg_hbi')

    csql = xlien.query("select  db,tb,csql from hbi_iceberg_table where `type`='增量'")
    for s in csql:
        csql = s[2]
        with open("./runsql.sql", 'w') as f:  # 如果filename不存在会自动创建， 'w'表示写数据，写之前会清空文件中的原有数据！
            f.write(s[2])
        execute(
            "/usr/hdp/3.0.1.0-187/spark3/bin/beeline -u jdbc:hive2://192.168.101.118:10010 -n olap -f /home/olap/ice/runsql.sql --force=true")
        xlien.execute("update hbi_iceberg_table set isok =1 where db=\'{db}\' and tb=\'{tb}\'".format(db=s[0], tb=s[1]))


def executeAndDoOverTime(cmd, db, tb, timeout=60 * 60 * 3):
    # 默认3小时超时
    p = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
    t_beginning = time.time()
    seconds_passed = 0
    flag = 0
    while True:
        if p.poll() is not None:
            break
        seconds_passed = time.time() - t_beginning
        if timeout and seconds_passed > timeout:
            p.terminate()

            raise TimeoutError(db, tb)
        time.sleep(1)
    return p.stdout.read()


def run_insert(unit):


    csql = xlien.query("""
    select a.db, a.tb, a.isql,hst.tb_size_unit,a.isok
from hbi_iceberg_insert a
         left join hbi_iceberg_table hit on a.db = hit.db and a.tb = hit.tb
         left join hbi_smallfile_table hst on a.db = hst.db and replace(a.tb,'_iceberg_68','') = hst.tb
where (hit.isok = 1 or  hit.isok =3)
  and a.isok = 0
  and hst.tb_size_unit = '{unit}'
    """.format(unit=unit))
    for s in csql:
        db = s[0]
        tb = s[1]
        isql = s[2]
        tb_size_unit = s[3]
        isok = s[4]
        print(db, tb, isql, tb_size_unit, isok)
        with open("./runsql.sql", 'w') as f:  # 如果filename不存在会自动创建， 'w'表示写数据，写之前会清空文件中的原有数据！
            f.write(isql)
        try:
            executeAndDoOverTime(
                "/usr/hdp/3.0.1.0-187/spark3/bin/beeline -u jdbc:hive2://192.118.101.18:10010 -n olap -f /home/olap/ice/runsql.sql --force=true",
                db, tb, timeout=3*60*60)
            xlien.execute(
                "update hbi_iceberg_insert set isok =1 where db=\'{db}\' and tb=\'{tb}\'".format(db=db, tb=tb))
        except:
            print("超时拉 555~！")


run_insert("G")