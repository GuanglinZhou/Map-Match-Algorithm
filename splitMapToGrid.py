#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-18 13:44:18
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$


'''
将路网划分为网格，并将九宫格大网格中对应的路段信息保存为表。
表名称为gridOfMapTable
字段为：
gridId,bigGridMinLatitude,bigGridMinLongitude,bigGridMaxLatitude,bigGridMaxLongitude,centralLatitude,centralLongitude,sectionID,startNode,startLatitude,startLongitude,endNode,endLatitude,endLongitude
表示'网格ID'，'大网格左下角对应纬度'，'大网格左下角对应经度'，'大网格右上角对应纬度'，'大网格右上角对应经度'，'网格中心点纬度'，'网格中心点经度'，'路段ID'...等
'''

import os
import pandas as pd
from math import radians, asin, sqrt, sin, cos
from collections import defaultdict
import pickle


# 利用haversine公式求两经纬度点间的的距离(单位:m)
def haversine(lat1, lon1, lat2, lon2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）

    # 将十进制度数转化为弧度
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    r = 6367  # 地球平均半径，单位为公里
    return 2 * asin(sqrt(sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2)) * r * 1000


# 构造路网的网格信息
def gridInfo(M, N):
    global min_lat, min_long, max_lat, max_long

    unitLat = (max_lat - min_lat) / M
    unitLong = (max_long - min_long) / N
    print(unitLat)
    print(unitLong)
    print('每个网格大小为：{}*{}'.format(haversine(min_lat, min_long, min_lat, min_long + unitLong),
                                 haversine(min_lat, min_long + unitLong, min_lat + unitLat, min_long + unitLong)))
    # 筛选出网格顶点，边以及正常的网格。
    # 顶点网格只能扩充为4网格，边网格扩充为6网格，正常网格扩充为9网格
    vertexGridIndexList = []
    # sideGridIndexList = []
    normalGridIndexList = []
    allGridIndexSet = set()
    # 左侧边索引集合(包含左侧两个顶点)
    leftGridIndexSet = set()
    # 右侧边索引集合(包含右侧两个顶点)
    rightGridIndexSet = set()
    for index in range(1, M * N + 1):
        allGridIndexSet.add(index)
        # 左侧顶点
        if (index == 1 or index == (M - 1) * N + 1):
            vertexGridIndexList.append(index)
            leftGridIndexSet.add(index)
            continue
        # 右侧顶点
        elif (index == N or index == M * N):
            vertexGridIndexList.append(index)
            rightGridIndexSet.add(index)
            continue
        # 上侧下侧边网格（不包括顶点）
        elif (1 < index < N or ((M - 1) * N + 1 < index and index < M * N)):
            pass
        # 左侧边网格（不包括顶点，if顶点，进入上面顶点的条件语句了）
        elif ((index - 1) % N == 0):
            leftGridIndexSet.add(index)
        # 右侧边网格
        elif (index % N == 0):
            rightGridIndexSet.add(index)
        # 非顶点和边网格
        else:
            normalGridIndexList.append(index)
    with open('{}-{}_gridInfo.csv'.format(M, N), 'w') as gridInfo:
        gridInfo.write(
            'gridID,bigGridMinLatitude,bigGridMinLongitude,bigGridMaxLatitude,bigGridMaxLongitude,')
        gridInfo.write('centralLatitude,centralLongitude')
        for gridID in range(1, M * N + 1):
            gridInfo.write('\n')
            # 全都扩充为九宫格网格，再判断就九个网格哪些存在于allGridIndexSet中。注意，针对最左侧网格需要剔除扩充后大网格中出现最右侧网格的情况，最右侧网格同理。
            # 这方法对行/列数<=2的不适用，不过路网某维度不会只有2个网格，适用于本情况。
            # 扩充后大网格所包含小网格的索引存放于expandGridIndexSet中
            expandGridIndexSet = set()
            expandGridIndexSet.update(
                [gridID - 1, gridID, gridID + 1, gridID - N - 1, gridID - N, gridID - N + 1, gridID + N - 1, gridID + N,
                 gridID + N + 1])
            expandGridIndexSet = expandGridIndexSet & allGridIndexSet
            if (gridID in leftGridIndexSet):
                expandGridIndexSet = expandGridIndexSet - rightGridIndexSet
            elif (gridID in rightGridIndexSet):
                expandGridIndexSet = expandGridIndexSet - leftGridIndexSet
            expandGridIndexList = list(expandGridIndexSet)
            expandGridIndexList.sort()
            maxGridIndex = expandGridIndexList[-1]
            minGridIndex = expandGridIndexList[0]
            if (gridID == 60450):
                print('{}:{}'.format(gridID, expandGridIndexSet))
                print(minGridIndex)
                print(maxGridIndex)
            # print('{}:{},{}'.format(gridID, minGridIndex, maxGridIndex))
            bigGridMinLongitude = ((minGridIndex % N) - 1) * unitLong + min_long
            bigGridMinLatitude = (int(minGridIndex / N)) * unitLat + min_lat
            if (maxGridIndex % N == 0):
                bigGridMaxLongitude = N * unitLong + min_long
                bigGridMaxLatitude = int(maxGridIndex / N) * unitLat + min_lat
            else:
                bigGridMaxLongitude = (maxGridIndex % N) * unitLong + min_long
                bigGridMaxLatitude = (int(maxGridIndex / N) + 1) * unitLat + min_lat
            if (gridID % N == 0):
                centralLongitude = (N - 1 + 0.5) * unitLong + min_long
                centralLatitude = (int(gridID / N) - 1 + 0.5) * unitLat + min_lat
            else:
                centralLongitude = ((gridID % N) - 1 + 0.5) * unitLong + min_long
                centralLatitude = (int(gridID / N) + 0.5) * unitLat + min_lat
            gridInfo.write(
                '{},{},{},{},{},{},{}'.format(gridID, bigGridMinLatitude, bigGridMinLongitude, bigGridMaxLatitude,
                                              bigGridMaxLongitude, centralLatitude, centralLongitude))
    return unitLat, unitLong


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


'''
将路网中的路段ID加入到网格路网表中
外层循环迭代网格
    内层循环迭代路段
输出字典dict{}
key=gridID
value=[[sectionID,roadName,startNode,startLatitude,startLongitude,endNode,endLatitude,endLongitude],[...],...]
'''


# 输入经纬度，返回其网格索引

def returnGridIndex(latitude, longitude):
    global unitLong, unitLat, min_lat, min_long, max_lat, max_long, N
    if (latitude < min_lat or longitude < min_long or latitude > max_lat or longitude > max_long):
        raise RuntimeError('{},{}超出路网范围'.format(latitude, longitude))
    gridIndex = int((latitude - min_lat) / unitLat) * N + int((longitude - min_long) / unitLong + 1)
    return gridIndex


def gridOfMapTable():
    global gridID_sectionInfo_dict, M, N
    gridInfo = pd.read_csv('{}-{}_gridInfo.csv'.format(M, N))
    sectionAndNode = pd.read_csv(
        projectDirectory + 'processDataFromOSM/table/sectionAndNode.csv')
    indexRangeGrid = range(0, gridInfo.shape[0])
    indexRangeSection = range(0, sectionAndNode.shape[0])
    print('网格总数：{}'.format(gridInfo.shape[0]))
    for indexGrid in indexRangeGrid:
        print('正在处理第{}个网格'.format(indexGrid + 1))
        gridID = gridInfo.loc[indexGrid, 'gridID']
        bigGridMinLatitude = gridInfo.loc[indexGrid, 'bigGridMinLatitude']
        bigGridMinLongitude = gridInfo.loc[indexGrid, 'bigGridMinLongitude']
        bigGridMaxLatitude = gridInfo.loc[indexGrid, 'bigGridMaxLatitude']
        bigGridMaxLongitude = gridInfo.loc[indexGrid, 'bigGridMaxLongitude']
        for indexSection in indexRangeSection:
            secID = sectionAndNode.loc[indexSection, 'secID']
            roadName = sectionAndNode.loc[indexSection, 'roadName']
            startNodeID = sectionAndNode.loc[indexSection, 'startNodeID']
            endNodeID = sectionAndNode.loc[indexSection, 'endNodeID']
            startNodeLatitude = sectionAndNode.loc[indexSection, 'startLatitude']
            startNodeLongitude = sectionAndNode.loc[indexSection, 'startLongitude']
            endNodeLatitude = sectionAndNode.loc[indexSection, 'endLatitude']
            endNodeLongitude = sectionAndNode.loc[indexSection, 'endLongitude']
            if (checkInGridRange(bigGridMinLatitude, bigGridMinLongitude, bigGridMaxLatitude, bigGridMaxLongitude,
                                 startNodeLatitude, startNodeLongitude) and checkInGridRange(bigGridMinLatitude,
                                                                                             bigGridMinLongitude,
                                                                                             bigGridMaxLatitude,
                                                                                             bigGridMaxLongitude,
                                                                                             endNodeLatitude,
                                                                                             endNodeLongitude)):
                tempList = []
                print('路段{}存在于网格{}中'.format(secID, gridID))
                tempList.append(secID)
                tempList.extend([roadName, startNodeID, startNodeLatitude, startNodeLongitude, endNodeID,
                                 endNodeLatitude, endNodeLongitude])
                gridID_sectionInfo_dict[gridID].append(tempList)

    with open('{}-{}_gridID_sectionInfo_dict.pickle'.format(M, N), 'wb') as file:
        pickle.dump(dict(gridID_sectionInfo_dict), file, protocol=pickle.HIGHEST_PROTOCOL)


if __name__ == '__main__':
    projectDirectory = '/Users/guanglinzhou/Documents/gpsDataProject/'
    # 将路网划分为M*N个网格(路网高(纬度方向)120km，宽(经度方向)87km，则M=600,N=435,差不多为200*200的网格)
    M = 397
    N = 290
    # M = 10
    # N = 10
    # 路网范围
    min_lat = 31.2832330
    max_lat = 32.3576973
    min_long = 116.8899771
    max_long = 117.8087044
    # M = 5
    # N = 3
    # 路网范围
    # min_lat = 0
    # max_lat = 5
    # min_long = 0
    # max_long = 3
    gridID_sectionInfo_dict = defaultdict(lambda: [])
    unitLat, unitLong = gridInfo(M, N)
    gridOfMapTable()
