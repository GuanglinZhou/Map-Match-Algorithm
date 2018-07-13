#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-18 13:44:18
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

import os
import pandas as pd
from math import radians, asin, sqrt, sin, cos
from collections import defaultdict

projectDirectory = '/Users/guanglinzhou/Documents/gpsDataProject/'

sectionMapFromOSM = pd.read_csv(
    projectDirectory + 'processDataFromOSM/table/sectionMapFromOSM.csv')
nodeMapFromOSM = pd.read_csv(
    projectDirectory + 'processDataFromOSM/table/nodeMapFromOSM.csv')

indexRangeSection = range(0, sectionMapFromOSM.shape[0])
with open(projectDirectory + 'processDataFromOSM/table/sectionAndNode.csv', 'w') as file:
    file.write(
        'secID,wayID,roadID,roadName,startNodeID,startLatitude,startLongitude,endNodeID,endLatitude,endLongitude')
    for indexSection in indexRangeSection:
        file.write('\n')
        if (indexSection % 100 == 0):
            print(indexSection)
        secID = sectionMapFromOSM.loc[indexSection, 'secID']
        wayID = sectionMapFromOSM.loc[indexSection, 'wayID']
        roadID = sectionMapFromOSM.loc[indexSection, 'roadID']
        roadName = sectionMapFromOSM.loc[indexSection, 'roadName']
        startNodeID = sectionMapFromOSM.loc[indexSection, 'startNodeID']
        endNodeID = sectionMapFromOSM.loc[indexSection, 'endNodeID']
        startNodeLatitude = nodeMapFromOSM[nodeMapFromOSM['nodeID'] == startNodeID]['latitude'].values[0]
        startNodeLongitude = nodeMapFromOSM[nodeMapFromOSM['nodeID'] == startNodeID]['longitude'].values[0]
        endNodeLatitude = nodeMapFromOSM[nodeMapFromOSM['nodeID'] == endNodeID]['latitude'].values[0]
        endNodeLongitude = nodeMapFromOSM[nodeMapFromOSM['nodeID'] == endNodeID]['longitude'].values[0]
        file.write(
            '{},{},{},{},{},{},{},{},{},{}'.format(secID, wayID, roadID, roadName, startNodeID, startNodeLatitude,
                                                   startNodeLongitude, endNodeID, endNodeLatitude, endNodeLongitude))
