# -*- coding: utf-8 -*-

import json
import math
from pprint import pprint
import time

from jsonFormat.dataserver import Mysqldb


db_red = {
    'host': '192.168.9.84',
    'user': 'root',
    'passwd': 'Biz_123456',
    'port': 3306,
    'db': 'biz_iplocation_db',
    'charset': 'utf8',
    'use_unicode': True
}
mysqldb = Mysqldb(db_red)


def select_data(ipv4):

    sql = """
     SELECT areacode, lat ,lon , country ,province ,city ,district , accuracy  
     FROM biz_iplocation_db.geoip_offline 
     WHERE minip <= INET_ATON("%s") ORDER BY minip DESC LIMIT 1
    """ % ipv4

    # print(sql)
    res = mysqldb.select(sql)

    return res


def insert_data(datalist):
    table = "biz_iplocation_db.clntest"
    insert = 'insert into ' + table + ' (ipv4, ipv6, asn_v4,asn_v6,country_code,day,latitude,longitude,prefix_v4,' \
                                      'prefix_v6, status_name, aw_areacode, aw_latitude, aw_longitude, aw_country, ' \
                                      'aw_province, aw_city, aw_district, aw_accuracy, verify_flag, distance )' \
                                      ' values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    try:
        mysqldb.executemany(insert, datalist)
        print('insert batch data over')
        return True
    except Exception as e:
        print('insert tht data error:', e)
        print(f'get error,please check')
        print('This task failed, update task status')
        return ''


def get_distance(lat1, lon1, lat2, lon2):
    # 将经纬度从度转换为弧度
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    # 应用 Haversine 公式计算距离
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = 6371 * c  # 地球半径为 6371 公里
    # 输出换算为米
    return distance*1000


def main():
    with open("20240401.json", encoding="utf-8") as f:
        text = json.loads(f.read())

        print(text.keys())
        print(text.get("meta"))
        # pprint(text.get("objects")[:10])

    info = text.get("objects")

    tmp_data_list = list()
    for items in info:
        ipv4 = items.get("address_v4")
        # 只获取IPv4有数据的
        if ipv4:
            data = (ipv4, items.get("address_v6"), items.get("asn_v4"), items.get("asn_v6"), items.get("country_code"),
                    items.get("day"), items.get("latitude"), items.get("longitude"), items.get("prefix_v4"),
                    items.get("prefix_v4"), items.get("status_name"))
            result = select_data(ipv4)
            data_1 = data + result[0]
            try:
                if items.get("latitude") and items.get("longitude") and result[0][1] and result[0][2]:
                    aw_lat = float(result[0][1])
                    aw_lon = float(result[0][2])
                    distance = get_distance(float(items.get("latitude")), float(items.get("longitude")), aw_lat, aw_lon)
                else:
                    distance = ""

                if distance and distance <= 50000:
                    flag = 1
                else:
                    flag = 0

                data_res = data_1 + (flag, str(distance), )
                # print(data_res)
                tmp_data_list.append(data_res)
            except Exception as e:
                print(e)
                print(ipv4)
                print(result[0])

        if len(tmp_data_list) == 200:
            # 每批插入200条数据
            insert_data(tmp_data_list)
            tmp_data_list.clear()

    print(len(tmp_data_list))
    print(tmp_data_list)
    # 最后的不满200个的
    insert_data(tmp_data_list)


if __name__ == '__main__':

    t1 = time.time()
    main()
    t2 = time.time()

    print("耗时：", t2-t1)




