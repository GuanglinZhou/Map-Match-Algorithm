#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-16 18:24:00
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

# 获取道路表格,列名如下：
# roadID,路名，WayID，类型(secondary)，user,version,timestamp

import os
import xml.etree.cElementTree as ET
from collections import defaultdict
import re
import pandas as pd


# 路网中有的路名仅使用了拼音，检测并剔除
def contain_zh(word):
    '''
    判断传入字符串是否包含中文
    :param word: 待判断字符串
    :return: True:包含中文  False:不包含中文
    '''
    zh_pattern = re.compile(u'[\u4e00-\u9fa5]+')
    match = zh_pattern.search(word)
    return match


# 返回路网中所有路名
def getRoadNameSet(tree):
    roadNameSet = set()
    roadTypeList = ['primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'trunk',
                    'trunk_link']
    for elem in tree.iter(tag='way'):
        tag_k = elem.findall('tag')
        for tag in tag_k:
            if (tag.attrib['k'] == 'highway' and tag.attrib['v'] not in roadTypeList):
                break
            if (tag.attrib['k'] == 'name'):
                roadName = tag.attrib['v']
                if (contain_zh(roadName)):
                    roadNameSet.add(roadName)
                else:
                    print(roadName)
    return roadNameSet


# 根据路名获取路网中道路相关的路段wayid,构建道路表
def constructRoadDataFrame(tree, roadNameSet):
    # 当出现多人上传了某类型路段，则选择topK个最新的记录
    roadDataFrame = pd.DataFrame(columns=['roadName', 'wayID', 'roadType', 'user', 'timestamp', 'version'])
    roadTypeList = ['primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'trunk',
                    'trunk_link']
    roadInfo = defaultdict(lambda: 0)
    roadType = ''
    roadNum = 0
    for roadName in roadNameSet:
        print('正在处理第{}条路： {}'.format(roadNum, roadName))
        roadNum += 1
        for elem in tree.iter(tag='way'):
            tag_k = elem.findall('tag')
            for tag in tag_k:
                #                 if (tag.attrib['k'] == 'highway'):
                #                     roadType = tag.attrib['v']
                if (tag.attrib['k'] == 'highway' and tag.attrib['v'] in roadTypeList):
                    roadType = tag.attrib['v']

                #                 print('roadType:{}'.format(roadType))
                if (tag.attrib['k'] == 'name'):
                    if (tag.attrib['v'] == roadName):
                        roadInfo['roadName'] = roadName
                        roadInfo['wayID'] = elem.attrib['id']
                        roadInfo['roadType'] = roadType
                        roadInfo['user'] = elem.attrib['user']
                        roadInfo['timestamp'] = elem.attrib['timestamp']
                        roadInfo['version'] = elem.attrib['version']
                        tempDF = pd.DataFrame([roadInfo])
                        roadDataFrame = pd.concat([roadDataFrame, tempDF], ignore_index=True)

    return roadDataFrame


# 先按照roadType聚合，在同一个roadType中，按照用户聚合，


def constructRoadTable(tree, roadDataFrame):
    topK = 3

    def func(dfGroupByRoadType):
        timestamp_index_dict = defaultdict(lambda: 0)
        global count
        global roadTable
        if (count != 0):
            print(dfGroupByRoadType)
            for user, dfGroupByRoadTypeGroupByUser in dfGroupByRoadType.groupby('user'):
                dfGroupByRoadTypeGroupByUser.sort_values('timestamp', ascending=False, inplace=True)
                timestamp_index_dict[
                    dfGroupByRoadTypeGroupByUser.iloc[0]['timestamp']] = dfGroupByRoadTypeGroupByUser.iloc[0][
                    'index']
            timestamp_index_list = sorted(timestamp_index_dict.items(), reverse=True)
            rang = topK if (topK < len(timestamp_index_list)) else len(timestamp_index_list)
            for i in range(rang):
                roadTable = pd.concat(
                    [roadTable, roadDataFrame[roadDataFrame['index'] == [timestamp_index_list[i][1]]]],
                    ignore_index=True)

        count += 1

    roadDataFrame.reset_index(inplace=True)
    roadDataFrame.groupby('roadType').apply(func)


if __name__ == '__main__':
    tree = ET.ElementTree(file='hefei_highways.osm')
    root = tree.getroot()
    roadNameSet = getRoadNameSet(tree)
    # roadNameSet = set(['翡翠路', '徽州大道', '长江路中', '南一环路', '芜湖路'])
    # roadNameSet = set(['芜湖路'])
    roadTable = pd.DataFrame(columns=['index', 'roadName', 'wayID', 'roadType', 'user', 'timestamp', 'version'])
    roadDataFrame = constructRoadDataFrame(tree, roadNameSet)
    for roadName, df in roadDataFrame.groupby('roadName'):
        count = 0
        constructRoadTable(tree, df)
    del roadTable['index']
    roadTable.reset_index(inplace=True)
    roadTable.rename(columns={'index': 'roadID'}, inplace=True)
    roadTable['roadID'] = roadTable['roadID'].apply(lambda x: 'R' + str(x))
    # roadTable['roadID'] = 'R' + roadTable['roadID']
    roadTable.to_csv('table/roadMapFromOSM.csv', index=False)
