'''
Created on Jul 23, 2019

@author: aleksei1985
'''
def connectToES(host= 'localhost',port = 9200):
    from elasticsearch import Elasticsearch
    return Elasticsearch([{'host':host,'port':port}])

def writeMultipleUserInfo(db, idsOfInterest):
    idsOfInterestTemp = []
    for id in idsOfInterest:
        idsOfInterestTemp.append(id.lower())
    idsOfInterest = idsOfInterestTemp
    
    es=connectToES()
    
    from elasticsearch.helpers import bulk
    collectionsOfInterest = ["locationDistributions", "raceDistributions", "genderDistributions", "countryDistributions", "languageDistributions", "timeDistributions"]
    for collectionName in collectionsOfInterest:
        collection = db[collectionName]
        
        condition = []
        for id in idsOfInterest:
            condition.append({'_id': id})
        query = {"$or": condition}

        tweetCursor = collection.find(query)
    
        recordsToWriteToESinBulk = []
        
        indexName = collectionName.lower()
        if idsOfInterest != None:
            if len(idsOfInterest) > 1:
                indexName += "select"
        else:
            indexName += "full"
        
        userInfoCollection = []
        if collectionName in ["locationDistributions"]:
            mappings = """
            {
              "mappings": {
                "properties": {
                  "location": {
                    "type": "geo_point"
                  }
                }
              }
            }
            """
            es.indices.delete(index=indexName, ignore=[400, 404])
            
            es.indices.create(index=indexName, body=mappings)

            import pickle
            filePath = "./CitiesFromGeoNamesToSearchFor.pickle"
            with open(filePath, "rb") as fp:
                list = pickle.load(fp)
                GEOstringRepresentationToID = list[0]
                GEOIDToValues = list[1]
            
            for userInfo in tweetCursor:
                recordID = False
                if idsOfInterest != None:
                    if userInfo["_id"] in idsOfInterest:
                        recordID = True
                else:
                    recordID = True
                
                if recordID:
                    userInfoCollection.append(userInfo)
                    for key in userInfo:
                        if not str(key) in ["_id", "totalRecords"]:
                            frequency = userInfo[key]

                            key = float(key)

                            geoname = key
                            dictHoldingValues = GEOIDToValues[geoname]
                            dictHoldingValues['term'] = GEOIDToValues[geoname]['City']+ " " + GEOIDToValues[geoname]['Country']
                            dictHoldingValues["frequency"] = float(frequency)
                            dictHoldingValues['str_id'] = geoname
                            dictHoldingValues["label"] = userInfo["_id"]
                            dictHoldingValues["location"] = {"lat": GEOIDToValues[geoname]["lat"], "lon": GEOIDToValues[geoname]["long"]} 
                            dictHoldingValues["_index"] = indexName
                            #dictHoldingValues["_type"] = "loc"
                            recordsToWriteToESinBulk.append(dictHoldingValues)
                
                            if len(recordsToWriteToESinBulk) > 10000:
                                bulk(es, recordsToWriteToESinBulk, raise_on_error=True)
                                recordsToWriteToESinBulk = []
        else:
            es.indices.delete(index=indexName, ignore=[400, 404])

            for userInfo in tweetCursor:
                recordID = False
                if idsOfInterest != None:
                    if userInfo["_id"] in idsOfInterest:
                        recordID = True
                else:
                    recordID = True
                
                if recordID:
                    userInfoCollection.append(userInfo)
                    if collectionName == "timeDistributions":
                        userInfoForElasticSearch = {}
                        userInfoForElasticSearch["term"] = 0
                        userInfoForElasticSearch["label"] = userInfo["_id"]
                        userInfoForElasticSearch["frequency"] = float(userInfo['0']+userInfo['24'])
                        userInfoForElasticSearch["_index"] = indexName
                        userInfoForElasticSearch["_type"] = indexName
                        recordsToWriteToESinBulk.append(userInfoForElasticSearch)
                    for key in userInfo:
                        if not str(key) in ["_id", "totalRecords", "0", "24"]:
                            userInfoForElasticSearch = {}
                            if collectionName == "timeDistributions":
                                userInfoForElasticSearch["term"] = float(key)
                            else:
                                userInfoForElasticSearch["term"] = key
                            
                            userInfoForElasticSearch["label"] = userInfo["_id"]
                            userInfoForElasticSearch["frequency"] = float(userInfo[key])
                            userInfoForElasticSearch["_index"] = indexName
                            userInfoForElasticSearch["_type"] = indexName
                            recordsToWriteToESinBulk.append(userInfoForElasticSearch)
                            
                            if len(recordsToWriteToESinBulk) > 10000:
                                bulk(es, recordsToWriteToESinBulk, raise_on_error=True)
                                recordsToWriteToESinBulk = []
        if len(recordsToWriteToESinBulk) > 0:
            bulk(es, recordsToWriteToESinBulk, raise_on_error=True)
            recordsToWriteToESinBulk = []
        print "written records to elastic search"
        
        import time
        time.sleep(5)
        res = es.search(index = indexName, body = {'query': {'match_all':{}}})
        numResults = res['hits']['total']
        print "total records written: " + str(numResults)

def writeAllUserInfo():
    db_name = 'twitter_demographics_across_followers'
    from pymongo import MongoClient
    host= 'localhost'
    port = 27017
    client = MongoClient(host, port) #connect to MongoDB server
    db = client[db_name]
    writeMultipleUserInfo(db, ["information_across_all_twitter_users"])

