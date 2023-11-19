from pymongo import MongoClient
import json
client = MongoClient('mongodb://localhost:27017/')
db = client['bdprojectdb']
coll = db['metadata']  # Use db to access the collection

filename = input("File  Name : ")
dict = {}
dict[filename] = {}
blk = {}
for _ in range(2):
    block = input("block number: ")
    ips = input("ips : ").split()
    temp = {}
    temp[block] = ips
    blk.update(temp)
dict[filename] = blk

print(dict)
coll.insert_one(dict)
client.close()