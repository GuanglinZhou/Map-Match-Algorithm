#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-17 08:59:37
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

# 获取节点表格,列名如下：
# nodeID,latitude,longitude
import os
import xml.etree.cElementTree as ET
import pandas as pd


def constructNodeTable(tree, sectionMapFromOSM):
    with open('table/nodeMapFromOSM.csv', 'w') as sectionFile:
        sectionFile.write('nodeID,latitude,longitude')
        nodeSet = set()
        for index in range(sectionMapFromOSM.shape[0]):
            nodeSet.add(sectionMapFromOSM.iloc[index][3])
            nodeSet.add(sectionMapFromOSM.iloc[index][4])
        nodeList = list(nodeSet)
        print('总共{}个节点'.format(len(nodeList)))
        num = 0
        for node in nodeList:
            for elem in tree.iter(tag='node'):
                if (elem.attrib['id'] == str(node)):
                    latitude = elem.attrib['lat']
                    longitude = elem.attrib['lon']
                    sectionFile.write('\n')
                    sectionFile.write(
                        '{},{},{}'.format(node, latitude, longitude))
                    num += 1
                    if (num % 100 == 0):
                        print('已处理了{}个节点'.format(num))


if __name__ == '__main__':
    tree = ET.ElementTree(file='hefei_highways.osm')
    root = tree.getroot()
    sectionMapFromOSM = pd.read_csv('table/sectionMapFromOSM.csv')
    constructNodeTable(tree, sectionMapFromOSM)
