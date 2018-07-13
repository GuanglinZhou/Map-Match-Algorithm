#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-17 08:13:37
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

# 获取路段表格,列名如下：
# wayID，roadID，roadName,startNodeID,endNodeID
import xml.etree.cElementTree as ET
import pandas as pd


def constructSectionTable(tree, roadMapFromOSM):
    with open('table/sectionMapFromOSM.csv', 'w') as sectionFile:
        sectionFile.write('secID,wayID,roadID,roadName,startNodeID,endNodeID')
        secIndex = 0
        for index in range(roadMapFromOSM.shape[0]):
            wayID = roadMapFromOSM.iloc[index][6]
            roadID = roadMapFromOSM.iloc[index][0]
            roadName = roadMapFromOSM.iloc[index][1]
            nodeIDList = []
            for elem in tree.iter(tag='way'):
                if (elem.attrib['id'] == str(wayID)):
                    nd_ref = elem.findall('nd')
                    for nd in nd_ref:
                        nodeIDList.append(nd.attrib['ref'])
            for num in range(len(nodeIDList) - 1):
                secID = 'Sec' + str(secIndex)
                secIndex += 1
                sectionFile.write('\n')
                sectionFile.write(
                    '{},{},{},{},{},{}'.format(secID, wayID, roadID, roadName, nodeIDList[num], nodeIDList[num + 1]))


if __name__ == '__main__':
    tree = ET.ElementTree(file='hefei_highways.osm')
    root = tree.getroot()
    roadMapFromOSM = pd.read_csv('table/roadMapFromOSM.csv')
    constructSectionTable(tree, roadMapFromOSM)
