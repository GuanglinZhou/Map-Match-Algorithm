import pandas as pd
from math import radians, cos, sin, asin, sqrt, atan2

'''
从Jin数据（训练集）中，将路名提取出来，并和路网数据roadMapFromOSM.csv中的路名比对（因为路名以路网中的路名为基准）
路名转换后存放在/Users/guanglinzhou/Documents/gpsDataProject/processDataFromJin/trainDataByRoadName.csv，
和/Users/guanglinzhou/Documents/gpsDataProject/processDataFromJin/roadNameInTrainCorrespondInMap.txt相结合，就可以将路名转换为对应的路段ID了，即构建标准训练集的过程
这部分代码放于/Users/guanglinzhou/Documents/gpsDataProject/processDataFromJin/constructTrainDataSet.py
'''


# 利用haversine公式求两经纬度点间的的距离(单位:m)
def haversine(lat1, lon1, lat2, lon2):  # 经度1，纬度1，经度2，纬度2 （十进制度数）

    # 将十进制度数转化为弧度
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    r = 6367  # 地球平均半径，单位为公里
    return 2 * asin(sqrt(sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2)) * r * 1000


def processRaodInTrain(roadName, index, drop_index_list):
    if ('区' in roadName and '路' in roadName):
        qu_index = roadName.find('区')
        lu_index = roadName.find('路')
        road = roadName[qu_index + 1: lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('区' in roadName and '道' in roadName):
        qu_index = roadName.find('区')
        lu_index = roadName.find('道')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('区' in roadName and '街' in roadName):
        qu_index = roadName.find('区')
        lu_index = roadName.find('街')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('区' in roadName and '巷' in roadName):
        qu_index = roadName.find('区')
        lu_index = roadName.find('巷')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('区' in roadName and '桥' in roadName):
        qu_index = roadName.find('区')
        lu_index = roadName.find('桥')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('区' in roadName and '线' in roadName):
        qu_index = roadName.find('区')
        lu_index = roadName.find('线')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('县' in roadName and '路' in roadName):
        qu_index = roadName.find('县')
        lu_index = roadName.find('路')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    if ('县' in roadName and '道' in roadName):
        qu_index = roadName.find('县')
        lu_index = roadName.find('道')
        road = roadName[qu_index + 1:lu_index + 1]
        pos = road.find('(')
        if (pos != -1):
            road = road[pos + 1:]
        return road
    # print('index: {},roadName: {}'.format(index, roadName))

    drop_index_list.append(index)
    return ' '


# 将train中有些路名进行过滤
def trainRoadName():
    gpsDataFromJin = pd.read_csv(projectDirectory + 'processDataFromJin/gpsDataFromJin1.csv')

    # indexRange = np.random.randint(gpsDataFromJin.shape[0], size=300)
    indexRange = range(gpsDataFromJin.shape[0])
    print('gpsDataRange:{}'.format(gpsDataFromJin.shape[0]))
    # indexRange = range(100)
    drop_index_list = []
    for index in indexRange:
        if (index % 100 == 0):
            print(index)
        road = gpsDataFromJin.iloc[index]['Address']
        newRoadName = processRaodInTrain(road, index, drop_index_list)
        gpsDataFromJin.set_value(index, 'Address', newRoadName)
    gpsDataFromJin.drop(drop_index_list, inplace=True)
    gpsDataFromJin.rename(columns={'Latitude': 'latitude', 'Longitude': 'longitude', 'Address': 'roadName'},
                          inplace=True)
    gpsDataFromJin = gpsDataFromJin[gpsDataFromJin['roadName'].notnull()]
    gpsDataFromJin = gpsDataFromJin[gpsDataFromJin['roadName'] != '']
    gpsDataFromJin.reset_index(drop=True, inplace=True)
    gpsDataFromJin.to_csv(projectDirectory + 'processDataFromJin/trainDataByRoadName1.csv',
                          index=False)

    trainRoadNameList = list(gpsDataFromJin['roadName'].unique())
    with open(projectDirectory + 'processDataFromJin/roadNameListInTrain.txt', 'w') as file:
        for roadName in trainRoadNameList:
            file.write('{}\n'.format(roadName))


def canBeDeleted():
    with open(projectDirectory + 'processDataFromJin/roadNameListInTrain.txt') as file:
        roadNameListInTrain = file.read().splitlines()
    with open(projectDirectory + 'processDataFromOSM/roadNameInRoadMap.txt') as file:
        roadNameListInMap = file.read().splitlines()
    print('train中路名个数为:{}'.format(len(roadNameListInTrain)))
    num = 0
    roadNameInTrainNotInMap = []
    roadNameInTrainInMap = []
    for roadNameInTrain in roadNameListInTrain:
        if (roadNameInTrain in roadNameListInMap):
            roadNameInTrainInMap.append(roadNameInTrain)
            num += 1
        else:
            roadNameInTrainNotInMap.append(roadNameInTrain)
    print('train中路名在路网中有:{},不在有:{}'.format(num, len(roadNameListInTrain) - num))
    print('在train不在map的有：')
    for roadName in roadNameInTrainNotInMap:
        print(roadName)
    file = open(projectDirectory + 'processDataFromJin/roadNameListInTrainInMap.txt', 'w')
    for item in roadNameInTrainInMap:
        file.write("%s,%s\n" % (item, item))


if __name__ == '__main__':
    # trainRoadName()
    # trainRoadName = pd.read_csv('/Users/guanglinzhou/Documents/gpsDataProject/processDataFromJin/trainRoadName.csv')
    # canBeDeleted()
    projectDirectory = '/home/zhou/Documents/gpsDataProject/'

    print('左下角到右下角：{}'.format(haversine(31.283233, 116.8899771, 31.283233, 117.8087044)))
    print('左上角到左下角：{}'.format(haversine(31.283233, 116.8899771, 32.3576973, 116.8899771)))
    trainRoadName()
