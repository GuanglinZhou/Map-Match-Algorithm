#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2018-05-02 10:10:49
# @Author  : guanglinzhou (xdzgl812@163.com)
# @Link    : https://github.com/GuanglinZhou
# @Version : $Id$

from itertools import groupby
from operator import itemgetter
import sys
from datetime import datetime
# import xgboost as xgb
# from sklearn import svm


def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.rstrip().split(separator, 1)


def mean(ilist):
    num = 0
    sum = 0
    for item in ilist:
        sum = sum + int(item)
        num += 1
    return float(sum) / num


def main(separator='\t'):
    data = read_mapper_output(sys.stdin, separator=separator)
    # groupby groups multiple word-count pairs by word,
    # and creates an iterator that returns consecutive keys and their group:
    #   current_word - string containing a word (the key)
    #   group - iterator yielding all ["<current_word>", "<count>"] items
    for carID, group in groupby(data, itemgetter(0)):
        try:

            # total_count = sum(int(count) for current_word, count in group)
            # longList=[]
            labelList = []
            for carID, label in group:
                if (label.split(',')[1] != 'None'):
                    temlist = []
                    # time = parser.parse(label.split(',')[0])
                    # temlist.append(time)
                    time = datetime.strptime(label.split(',')[0], "%Y/%m/%d %H:%M:%S")
                    temlist.append(time)
                    temlist.append(label.split(',')[1])
                    labelList.append(temlist)
                else:
                    pass
            # print("{}{}{}".format(carID, separator, label))
            labelList = sorted(labelList, key=lambda x: x[0])
            for item in labelList:
                time = item[0]
                sectionID = item[1]
                print("{}{}{},{}".format(carID, separator, time, sectionID))

            # print("{}{}{},{}".format(carID, separator, mean(longList),mean(exampleList)))
            num = 1
            # print("{}{}{}".format(carID, separator, num))
        except ValueError:
            # count was not a number, so silently discard this item
            pass


if __name__ == "__main__":
    main()
