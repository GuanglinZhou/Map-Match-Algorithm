#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-02 09:27:50
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

'''
mapper函数，<carID , timestamp、sectionID>
'''
import os
# from jpype import *
import sys
from math import radians, cos, sin, asin, sqrt
import pickle
import csv
from collections import defaultdict
import operator
from jpype import *


# import pandas as pd


# 返回XY坐标系中两点之间距离
def distanceNode2Node(x1, y1, x2, y2):
    return sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))


# 返回XY坐标系中，点到线段的距离
def distanceNode2Line(x1, y1, x2, y2, x_p, y_p):
    a = distanceNode2Node(x2, y2, x_p, y_p)
    if (a <= 0.00001):
        return 0.0
    b = distanceNode2Node(x1, y1, x_p, y_p)
    if (b <= 0.00001):
        return 0.0
    c = distanceNode2Node(x1, y1, x_p, y_p)
    if (c <= 0.00001):
        return a
    if (a * a >= b * b + c * c):
        return b
    if (b * b >= a * a + c * c):
        return a
    s = (a + b + c) / 2
    s = sqrt(s * (s - a) * (s - b) * (s - c))
    return 2 * s / c


# 利用haversine公式求两经纬度点间的的距离(单位:m)
def haversine(lat1, lon1, lat2, lon2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）

    # 将十进制度数转化为弧度
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    r = 6367  # 地球平均半径，单位为公里
    return 2 * asin(sqrt(sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2)) * r * 1000


# 得到以（minLongitude，minLatitude）为原点坐标系中，pos的XY坐标
def computeXY(minLatitude, minLongitude, posLatitude, posLongitude):
    # 点pos在Y轴（纬度）轴投影
    proLat_X, proLat_Y = minLongitude, posLatitude - minLatitude
    proLong_X, proLong_Y = posLongitude - minLongitude, minLatitude
    X = haversine(posLatitude, posLongitude, proLat_Y, proLat_X)
    Y = haversine(posLatitude, posLongitude, proLong_Y, proLong_X)
    return X, Y


# 给定某位置经纬度，和某路段的起始终止节点ID，返回该位置距离该路段的距离
def returnDistanceToOneSection(positionlatitude, positionlongtitude, startNodeLatitude, startNodeLongitude,
                               endNodeLatitude, endNodeLongitude):
    minLatitude = min([positionlatitude, startNodeLatitude, endNodeLatitude])
    minLongitude = min([positionlongtitude, startNodeLongitude, endNodeLongitude])
    postionX, positionY = computeXY(minLatitude, minLongitude, positionlatitude, positionlongtitude)
    startX, startY = computeXY(minLatitude, minLongitude, startNodeLatitude, startNodeLongitude)
    endX, endY = computeXY(minLatitude, minLongitude, endNodeLatitude, endNodeLongitude)
    return distanceNode2Line(startX, startY, endX, endY, postionX, positionY)


# 检查某个Node是否在大网格中
def checkInGridRange(gridMinLatitude, gridMinLongitude, gridMaxLatitude, gridMaxLongitude, NodeLatitude,
                     NodeLongitude):
    if (NodeLatitude < gridMaxLatitude \
            and NodeLatitude > gridMinLatitude \
            and NodeLongitude < gridMaxLongitude \
            and NodeLongitude > gridMinLongitude):
        return True
    else:
        return False












    


# 输入经纬度，返回其网格索引
def returnGridIndex(latitude, longitude):
    global M, N
    # 路网范围
    min_lat = 31.2832330
    max_lat = 32.3576973
    min_long = 116.8899771
    max_long = 117.8087044
    unitLat = (max_lat - min_lat) / M
    unitLong = (max_long - min_long) / N
    # if (latitude < min_lat or longitude < min_long or latitude > max_lat or longitude > max_long):
    #     raise RuntimeError('{},{}超出路网范围'.format(latitude, longitude))
    gridIndex = int((latitude - min_lat) / unitLat) * N + int((longitude - min_long) / unitLong + 1)

    return gridIndex


if __name__ == '__main__':
    M = 397
    N = 290
    gridID_sectionInfo_dict = defaultdict(lambda: [])
    count = 0
    with open('{}-{}_pickle.csv'.format(M, N), 'r') as file:
        ilist = []
        for line in file:
            if (count != 0):
                gridID = line.split(',')[0]
                sectionList = line.split(',')[1:]
                gridID_sectionInfo_dict[gridID].append(sectionList)
            count += 1
    # example=''
    # with open('example.txt','r') as file:
    #     for line in file:
    #         words=line.split(',')
    #         # print(line)
    #         example=example+str(words[0])
    num = 0
    for line in sys.stdin:
        if (num != 0):
            words = line.strip().split(',')
            carID = str(words[0])
            longitude = float(words[2])
            latitude = float(words[1])
            roadName = str(words[3])
            gridIndex = str(returnGridIndex(latitude, longitude))
            if (gridIndex in gridID_sectionInfo_dict.keys()):
                sectionInfoList = gridID_sectionInfo_dict[gridIndex]
                if (len(sectionInfoList) == 0):
                    label = None
                else:
                    secID_distance_dict = defaultdict(lambda: 0)
                    for sectionInfo in sectionInfoList:
                        distance = returnDistanceToOneSection(latitude, longitude, float(sectionInfo[3]),
                                                              float(sectionInfo[4]),
                                                              float(sectionInfo[6]), float(sectionInfo[7]))
                        secID_distance_dict[sectionInfo[0]] = distance
                    sort_secID_distance_list = sorted(secID_distance_dict.items(), key=operator.itemgetter(1))
                    label = sort_secID_distance_list[0][0]
                    print('{}\t{},{},{},{}'.format(carID, latitude, longitude, roadName, label))

            else:
                label = None
            # print('{}\t None'.format(carID))
        num += 1
