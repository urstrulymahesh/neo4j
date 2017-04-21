from glob import iglob
import os
import requests
import json
from py2neo import Graph,Path,authenticate,Node,Relationship

def commoncount(list1,list2):
    count = 0
    for element in list1:
        if element in list2:
            count +=1
    return count

authenticate("localhost:7474","neo4j","cmk110996")
graph = Graph("http://localhost:7474/db/data/")
array = []
for fname in iglob(os.path.expanduser('~/Downloads/test/*.json')):
    with open(fname) as fin:
        videos = json.load(fin)
        array.append(videos)
        stats = videos['videoInfo']['statistics']
        node = Node("Video",id = videos['videoInfo']['id'], commentCount = stats['commentCount'], viewCount = stats['viewCount'], favoriteCount = stats['favoriteCount'], likeCount = int(stats['likeCount']) , dislikeCount = stats['dislikeCount'])
        graph.create(node)
print("Nodes Finished")

for i in range(len(array)):
    temp = array[i]['videoInfo']
    for j in range(i-1,-1,-1):
        dup = array[j]['videoInfo']
        a = graph.find_one("Video",property_key = 'id', property_value = temp['id'])
        b = graph.find_one("Video",property_key = 'id', property_value = dup['id'])
        if temp['snippet']['channelId'] == dup['snippet']['channelId']:
            crelation = Relationship(a,"samechannel",b)
            graph.create(crelation)

        dcount = commoncount(temp['snippet']['description'].split(),dup['snippet']['description'].split())
        if dcount != 0:
            drelation = Relationship(a , "similardescription" , b , weight = dcount)
            graph.create(drelation)

        if 'tags' in temp['snippet'] and 'tags' in dup['snippet']:
            tcount = commoncount(temp['snippet']['tags'],dup['snippet']['tags'])
            if tcount != 0:
                trelation = Relationship(a, "similartags", b, weight = tcount)
                graph.create(trelation)
    print(i)


print("Finish")
