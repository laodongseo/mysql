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
        
 # 数据库
    dbconfig = dict(
        host="23.236.69.34",
        db="ciku",
        charset="utf8mb4",
        user="root",
        password = "wocaoseo2020",
        port=3306,
        # cursorclass=pymysql.cursors.DictCursor
    )

    connect = con_mysql()
    cursor = connect.cursor()  # 获取游标
