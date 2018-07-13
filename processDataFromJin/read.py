#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-04-02 18:10:31
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

import numpy as np
import pandas as pd


# inputfile = open('/Users/guanglinzhou/Documents/gpsDataProject/processDataFromJin/export.sql', 'r')
# outputfile = open('data.txt', 'w')
# i = 0
# for s in inputfile:
#     if (s.find('values') != -1):
#         index = s.index('values')
#         outputfile.write(s[index + 8:-3])
#         outputfile.write('\n')
#     i += 1
#     if (i % 100000 == 0):
#         print(i)
#     # print(s)
# inputfile.close()
# outputfile.close()
# 判断经纬度是否在路网范围内
def checkLatLongInRange(lat, lon):
    if (float(lat) < 31.283233 or float(lat) > 32.3576973):
        return False
    if (float(lon) < 116.8899771 or float(lon) > 117.8087044):
        return False
    return True


with open('gpsDataFromJin.csv', 'w') as csvFile:
    csvFile.write(
        'Latitude,' + 'Longitude,' + 'Address')
    with open('data.txt', 'r') as file:
        line_num = 0
        for line in file:
            # 从前往后按','分割读经纬度，从后往前查看'\''读路名，因为在START_ADDRESS_DESC和END_ADDRESS_DESC这两个字段可能包含不止一个','
            # 完全按照','split会出错
            infoList = line.split(',')
            start_lt = infoList[9]
            start_lg = infoList[11]
            end_lt = infoList[10]
            end_lg = infoList[12]
            quoNum = 0
            index3 = 0
            index4 = 0
            index7 = 0
            index8 = 0
            for i in reversed(range(len(line))):
                if (line[i] == '\''):
                    quoNum += 1
                    if (quoNum == 3):
                        index3 = i
                    if (quoNum == 4):
                        index4 = i
                    if (quoNum == 7):
                        index7 = i
                    if (quoNum == 8):
                        index8 = i
            start_address = line[index8 + 1:index7]
            end_address = line[index4 + 1:index3]
            line_num += 1
            if (line_num % 1000 == 0):
                # break
                print(line_num)
            if (checkLatLongInRange(start_lt,
                                    start_lg) and start_address != 'null' and ',' not in start_address and '安徽省合肥市' in start_address):
                csvFile.write('\n')
                csvFile.write('{},{},{}'.format(start_lt, start_lg, start_address))
            if (checkLatLongInRange(end_lt,
                                    end_lg) and end_address != 'null' and ',' not in end_address and '安徽省合肥市' in end_address):
                csvFile.write('\n')
                csvFile.write('{},{},{}'.format(end_lt, end_lg, end_address))
