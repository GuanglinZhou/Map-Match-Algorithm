import pandas as pd

# file = pd.read_csv('/Users/guanglinzhou/Documents/gpsDataProject/processDataFromOSM/table/roadMapFromOSM.csv')
# roadNameList = list(set(file['roadName'].tolist()))
# with open('/Users/guanglinzhou/Documents/gpsDataProject/processDataFromOSM/roadNameInRoadMap.txt', 'w') as file:
#     for roadName in roadNameList:
#         file.write('{}\n'.format(roadName))
projectDirectory = '/Users/guanglinzhou/Documents/gpsDataProject/'


# # 得到路网范围
# file = pd.read_csv(projectDirectory + 'processDataFromOSM/table/nodeMapFromOSM.csv')
# max_lat = file.max()['latitude']
# min_lat = file.min()['latitude']
# max_lon = file.max()['longitude']
# min_lon = file.min()['longitude']
# print('维度范围: {} ~ {}\n经度范围: {} ~ {}'.format(min_lat, max_lat, min_lon, max_lon))
# print(min_lat)
# print(max_lat)
# print(min_lon)
# print(max_lon)
# 判断经纬度是否在路网范围内
def checkLatLongInRange(lat, lon):
    if (float(lat) < min_lat or float(lat) > max_lat):
        return False
    if (float(lon) < min_lon or float(lon) > max_lon):
        return False
    return True


'''
根据经纬度范围，输出该范围内有哪些路名
'''
sectionAndNode = pd.read_csv(projectDirectory + 'processDataFromOSM/table/sectionAndNode.csv')
min_lat, min_lon = 31.862415267506297, 117.27964419620689
max_lat, max_lon = 31.87053464508816, 117.28914827172413
roadNameInRangeList = []
indexRange = range(sectionAndNode.shape[0])
for index in indexRange:
    if (index % 500 == 0):
        print(index)
    roadName = sectionAndNode.loc[index, 'roadName']
    startLatitude = sectionAndNode.loc[index, 'startLatitude']
    startLongitude = sectionAndNode.loc[index, 'startLongitude']
    endLatitude = sectionAndNode.loc[index, 'endLatitude']
    endLongitude = sectionAndNode.loc[index, 'endLongitude']
    if (checkLatLongInRange(startLatitude, startLongitude) and checkLatLongInRange(endLatitude, endLongitude)):
        roadNameInRangeList.append(roadName)

print(set(roadNameInRangeList))
