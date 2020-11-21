# -*- coding:UTF-8 -*-
"""
指定excel文件中的字段写入数据库
要求:excel字段和mysql字段名字必须一致,自增且主键的字段不必写
"""
import pandas
import pymysql


# 连接mysql
def con_mysql(dbconf,retry=2):
    try:
       conn = pymysql.connect(**dbconf)
    except Exception as e:
        print(e)
        if retry > 0:
            con_mysql(dbconf,retry-1)
    else:
        print('mysql成功连接')
        return conn


# 生成写入sql
def gen_sql(dict_data, mysql_sheet):
    keys = list(dict_data.keys())
    values = list(dict_data.values())
    # values = [pymysql.escape_string(s) for s in values]
    cols_insert = ','.join("`{0}`".format(i) for i in keys)
    cols_values = ','.join("'{0}'".format(i) for i in values)
    sql = """insert into {0} ({1}) VALUES({2});""".format(mysql_sheet, cols_insert, cols_values)
    return sql


if __name__ == "__main__":
    # 数据库
    dbconfig = dict(
        host="139.196.219.44",
        db="baidu_CIKU",
        charset="utf8mb4",
        user="root",
        password = "wocaoseo2020",
        port=3306,
        # cursorclass=pymysql.cursors.DictCursor
    )

    connect = con_mysql(dbconfig)
    cursor = connect.cursor()  # 获取游标
    # 数据表
    mysql_sheet = 'baidu_ciku'
    # 需要写入库的excel字段,主键且自增列不用写
    cols_list = ['kwd','max_click','domain_url']
    # 读取excel文件
    df = pandas.read_excel('kwd_txwzw8.com.xlsx')

    row_num, col_num = df.shape
    for i in range(0, row_num):
        df_row = df.loc[i]
        series_row = df_row[cols_list]
        dict_data = dict(series_row)
        sql = gen_sql(dict_data, mysql_sheet)
        print(sql)
        try:
            cursor.execute(sql)
            print('insert {0}条'.format(i+1))
        except Exception as e:
            print(e)
            exit()
        else:
            connect.commit()
    cursor.close()
    connect.close()
