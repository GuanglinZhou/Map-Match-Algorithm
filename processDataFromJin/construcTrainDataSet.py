#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-20 10:39:25
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

'''
每个网格中构建训练样本，如果网格中没有路段，则不构建
训练样本字段为：纬度，经度，距离某sec的距离,...,该样本所属的路段ID
Latitude,Longitude,secID1,...,label
'''

import pandas as pd
import numpy as np
from math import radians, cos, sin, asin, sqrt
from collections import defaultdict
import os
import pickle
import operator


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
    if (latitude < min_lat or longitude < min_long or latitude > max_lat or longitude > max_long):
        raise RuntimeError('{},{}超出路网范围'.format(latitude, longitude))
    gridIndex = int((latitude - min_lat) / unitLat) * N + int((longitude - min_long) / unitLong + 1)

    return gridIndex


def constructTrainDataSet():
    global M, N, directory, projectDirectory
    trainDataByRoadName = pd.read_csv(
        projectDirectory + 'processDataFromJin/trainDataByRoadName.csv')
    trainName_mapName_dict = {}
    with open(
            projectDirectory + 'processDataFromJin/roadNameInTrainCorrespondInMap.txt') as file:
        for line in file:
            index = line.find(',')
            key, val = line[:index], line[index + 1:-1].split(',')
            trainName_mapName_dict[key] = val
    with open(projectDirectory + '{}-{}_gridID_sectionInfo_dict.pickle'.format(M, N),
              'rb') as file:
        gridID_sectionInfo_dict = pickle.load(file)
    indexRange = range(trainDataByRoadName.shape[0])
    # indexRange = range(100)
    print('总计：{}'.format(trainDataByRoadName.shape[0]))
    for index in indexRange:
        if (index % 1000 == 0):
            print(index)
        trainRoadName = trainDataByRoadName.loc[index, 'roadName']
        # 由于路网信息不完善，很可能该训练样本的路名在路网中没有对应的路名
        mapNameList = trainName_mapName_dict[trainRoadName]
        if (mapNameList[0] == '无'):
            continue
        trainLatitude = trainDataByRoadName.loc[index, 'latitude']
        trainLongitude = trainDataByRoadName.loc[index, 'longitude']
        gridIndex = returnGridIndex(trainLatitude, trainLongitude)
        # 该网格中没有路段信息
        if (gridIndex not in gridID_sectionInfo_dict.keys()):
            # print('网格中没有路段信息')
            continue
        # 查看该网格中是否有训练集对应的路名
        mapRoadNameSet = set()
        sectionInfoList = gridID_sectionInfo_dict[gridIndex]
        for sectionInfo in sectionInfoList:
            mapRoadNameSet.add(sectionInfo[1])
        # 该网格中没有与训练集相对应的路名,如：训练集为金寨路，网格中有黄山路，绩溪路，则不符合情况
        if (len(set(mapNameList) & mapRoadNameSet) == 0):
            # print('网格中没有与训练集相对应的路名')
            continue
        # print('创建网格{}中的训练数据集'.format(gridIndex))
        # 如果网格对应训练集文件是否存在，不存在就返回False
        if (os.path.isfile(directory + '/trainData_{}.csv'.format(gridIndex))):
            trainData = pd.read_csv(directory + '/trainData_{}.csv'.format(gridIndex))
            secID_distance_dict = defaultdict(lambda: 0)
            distanceList = []
            for sectionInfo in sectionInfoList:
                distance = returnDistanceToOneSection(trainLatitude, trainLongitude, sectionInfo[3], sectionInfo[4],
                                                      sectionInfo[6], sectionInfo[7])
                secID_distance_dict[sectionInfo[0]] = distance
                distanceList.append(distance)
            sort_secID_distance_list = sorted(secID_distance_dict.items(), key=operator.itemgetter(1))

            label = sort_secID_distance_list[0][0]
            newRowTrainDataList = [trainLatitude, trainLongitude]
            newRowTrainDataList.extend(distanceList)
            newRowTrainDataList.append(label)
            newRowTrainDataDF = pd.DataFrame([newRowTrainDataList], columns=trainData.columns)
            trainData = pd.concat([trainData, newRowTrainDataDF])
            trainData.to_csv(directory + '/trainData_{}.csv'.format(gridIndex), index=False)

        else:
            with open(directory + '/trainData_{}.csv'.format(gridIndex), 'w') as trainData:
                trainData.write('latitude,longitude')
                secIDList = []
                for sectionInfo in sectionInfoList:
                    secIDList.append(sectionInfo[0])
                for secID in secIDList:
                    trainData.write(',' + secID)
                trainData.write(',label')
                trainData.write('\n{},{}'.format(trainLatitude, trainLongitude))
                secID_distance_dict = defaultdict(lambda: 0)
                for sectionInfo in sectionInfoList:
                    distance = returnDistanceToOneSection(trainLatitude, trainLongitude, sectionInfo[3], sectionInfo[4],
                                                          sectionInfo[6], sectionInfo[7])
                    secID_distance_dict[sectionInfo[0]] = distance
                    trainData.write(',{}'.format(distance))
                sort_secID_distance_list = sorted(secID_distance_dict.items(), key=operator.itemgetter(1))
                label = sort_secID_distance_list[0][0]
                trainData.write(',{}'.format(label))


if __name__ == '__main__':
    M = 397
    N = 290
    projectDirectory = '/home/zhou/Documents/gpsDataProject/'
    directory = projectDirectory + 'processDataFromJin/{}-{}_trainDataSet'.format(M, N)
    if not os.path.exists(directory):
        os.makedirs(directory)
    constructTrainDataSet()
    # print((returnGridIndex(31.7147666667, 117.16285)))
