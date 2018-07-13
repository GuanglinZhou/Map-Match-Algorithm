#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-17 10:39:25
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$


import os
import pandas as pd


# 将训练集中的label改成数字，即去除前面的Sec，这样可以避免在spark程序中使用labelIndexer这类东西

# def process(directory):
#     dir = projectDirectory + 'processDataFromJin/{}-{}_trainDataSetForSpark'.format(M, N)
#     if not os.path.exists(dir):
#         os.makedirs(dir)
#     indexRange = range(M * N)
#     print(indexRange)
#     for indexNum in indexRange:
#         if (indexNum % 1000 == 0):
#             print(indexNum)
#         fileName = directory + '/trainData_{}.csv'.format(indexNum)
#         if (os.path.exists(fileName)):
#             df = pd.read_csv(fileName)
#             df['label'] = df['label'].map(lambda X: X[3:])
#             df.to_csv('{}/trainDataForSpark_{}.csv'.format(dir, indexNum), index=False)

def process(directory):
    indexRange = range(M * N)
    print(indexRange)
    ilist = []
    # with open(projectDirectory + 'processDataFromJin/gridIDHaveData.txt', 'w') as file:
    for indexNum in indexRange:
        if (indexNum % 1000 == 0):
            print(indexNum)
        fileName = directory + '/trainData_{}.csv'.format(indexNum)
        if (os.path.exists(fileName)):
            # file = pd.read_csv(fileName)
            # file = file.drop_duplicates('latitude').reset_index(drop=True)
            # if (file.shape[0] == 1):
            #     os.remove(fileName)
            # else:
            #     file.to_csv(fileName, index=False)
            ilist.append(indexNum)
        # file.write('{},'.format(indexNum))
    print(ilist)
    print(len(ilist))


if __name__ == '__main__':
    M = 397
    N = 290
    projectDirectory = '/home/zhou/Documents/gpsDataProject/'
    directory = projectDirectory + 'processDataFromJin/{}-{}_trainDataSet'.format(M, N)
    process(directory)
