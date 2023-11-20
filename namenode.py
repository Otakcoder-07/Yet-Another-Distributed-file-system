import rpyc
from pymongo import MongoClient
import threading
from rpyc.utils.server import ThreadedServer
import bson
import traceback
import sys
from datetime import datetime
import time


datablocks = []


class NameNode(rpyc.Service):
    datanode_no = 0
    datanode_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    datanode_no1 = 0
    global datablocks
    def on_connect(self, conn):
        self.mongo = MongoClient('mongodb://localhost:27017/')
        db = self.mongo['bdprojectdb']
        self.coll = db['metadata'] 
        self.dn_coll = db['datanode_detail']
        print('Metadata connected')
        print("NameNode connected")

    def on_disconnect(self, conn):
        print("NameNode disconnected")

    def exposed_update_filename(self,file_name):
        self.filename = file_name
        dict = {}
        dict['file_name'] = file_name
        d = self.coll.find_one(dict)
        if(d == None):
            self.coll.insert_one(dict)
        else:
            print(f'file {self.filename} already exists')
            sys.exit(0)

    def exposed_write_file(self):
        # metadata = [
        #     {"ip_addr": "127.0.0.1", "port": 12346, "block_name": "0.txt"},
        #     {"ip_addr": "127.0.0.1", "port": 12347, "block_name": "50.txt"},
        #     {"ip_addr": "127.0.0.1", "port": 12348, "block_name": "100.txt"}
        # ]

        #loading metadata
        k = self.dn_coll.find_one({'id':self.datanode_id})
        if(k==None or k['block_locations']==None or k['block_locations']==[]):
            print("cant upload or download files now try again after some time")
            sys.exit(0)
        return {"block_locations": k['block_locations']}

    def exposed_receive_message(self, message):
        message['block_name'] = str(self.datanode_no1)+'.txt'
        self.datanode_no1 += 1
        self.datanode = []
        self.datanode.append(message)
        # print(f"Received message from client: {self.datanode}")

    def exposed_update_block_names(self,block_data):
        print('block_no : ',self.datanode_no)
        self.datanode_no += 1
        dock = self.coll.find_one({'file_name':self.filename})
        dock[str(self.datanode_no)] = list(block_data['data'])
        self.coll.update_one({'file_name': self.filename}, {'$set': {str(self.datanode_no): list(block_data['data'])}})

    def exposed_upload_complete(self):
        self.datanode_no = 0
        dock = self.coll.find_one({'file_name':self.filename})
        print(f'filename : {self.filename},block_data : {dock}')

    def exposed_get_blockname_data(self,filename):
        dock = self.coll.find_one({'file_name':filename})
        return dock

    def exposed_mark_datanode(self,ip_dn,port_dn):
        print(f'datanode :\nip : {ip_dn},port : {port_dn}')
        detail = {'ip_addr':ip_dn,'port':port_dn}
        dock = self.dn_coll.find_one({'id':self.datanode_id})
        if(dock):
            temp = dock['block_locations']
            temp.append(detail)
            self.dn_coll.update_one({'id':self.datanode_id},{'$set':{'block_locations':temp}})
        else:
            dict = {'id':self.datanode_id,'block_locations':[detail]}
            self.dn_coll.insert_one(dict)
            time.sleep(3)
            self.is_datanode_alive()

    def is_datanode_alive(self):
        dn_detail = self.dn_coll.find_one({'id':self.datanode_id})
        if(len(dn_detail['block_locations']) == 0):
            print('no datanode available')
            # sys.exit(0)
        else:
            print(f'dn in alive{dn_detail}')
            index = 0
            for dn_det in dn_detail['block_locations']:
                ip_addr = dn_det['ip_addr']
                port = dn_det['port']
                try:
                    rpyc.connect(ip_addr,port)
                except ConnectionRefusedError as e:
                    print(f'datanode with ip : {ip_addr} and port : {port} is lost and reason {e}')
                    dn_detail['block_locations'].pop(index)
                    self.dn_coll.update_one({'id':self.datanode_id},{'$set':{'block_locations':dn_detail['block_locations']}})
                finally:
                    index += 1
        time.sleep(5)
        self.is_datanode_alive()




if __name__ == "__main__":

    # # namenode = NameError()
    # # thre = threading.Thread(target = namenode)
    # t = ThreadedServer(NameNode(), port=12345)
    # t.start()
    try:
        t = ThreadedServer(NameNode(), port=12345)
        t.start()
    except Exception as e:
        print(f"Error starting NameNode server: {e}")
        traceback.print_exc()

