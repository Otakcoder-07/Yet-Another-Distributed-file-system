import rpyc
from pymongo import MongoClient
from rpyc.utils.server import ThreadedServer
import traceback
import sys
from datetime import datetime
import time
import socket

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
        self.dire_coll = db['directories']
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
    
    # def exposed_list_filename(self):
    #     cursor = self.coll.find({},{"file_name":1,"_id":0})
    #     d=[]
    #     for document in cursor:
    #         d.append(document['file_name'])
    #     return d

    def exposed_list_filename(self,path):
        print("path : ",path)
        if path == '/':
            cursor = self.dire_coll.find({})
            d=[]
            if not cursor.alive:
                return d
            for document in cursor:
                print(document)
                for key in document.keys():
                    d.append(key)
            d.remove('_id')
            return d

        elif '/' not in path:
            l = []
            dock = self.dire_coll.find_one({path:{'$exists':True}})
            if dock:
                for i in dock[path]:
                    for key in i.keys():
                        l.append(key)
                return l
            else: return False

        else:
            dir = path.split('/')
            dock = self.dire_coll.find_one({dir[0]:{'$exists':True}})
            dock2 = dock[dir[0]]
            temp = self.dir_working_for_listing(dir[1:],dir[-1],dock2,0)
            return temp


        
    def dir_working_for_listing(self,dir,endfile,dock2,i):
        print("hello : ",dock2)
        temp = next((obj for obj in dock2 if dir[i] in obj), None)
        # if temp == None:
        print('temp :',temp,"\n endfile : ",endfile,"\ndir : ",dir,"\ndock2: ",dock2,"\ni : ",i)
        if dir[i] == endfile:
            # temp_ = []
            if(temp):
                l = []
                # print(f"file {dir[i]} already exists")
                for i in temp[dir[i]]:
                    for key in i.keys():
                        l.append(key)
                return l
            else:
                if(temp == []): return True
                else: return False
            # dock2.append(end)
            # print(f"in dir if not {end}")
            # dock2.append(temp)
            # return dock2
        elif temp == None:
            print(f"No directory {dir[i]}")
            return None
        else:
            l = self.dir_working(dir,endfile,temp[dir[i]],i+1)
            # print(l)
            # print("dock2: ",dock2)
            # temp[dir[i]].extend(l)
            print('temp :',temp,"\n endfile : ",endfile,"\ndir : ",dir,"\ndock2: ",dock2,"\ni : ",i)

            # print(f"\n\nin dir if there {dock2}\n\n")
            return l


    # def exposed_delete_filename(self,file_name):
    #     self.filename = file_name
    #     dict = {}
    #     dict['file_name'] = file_name
    #     d = self.coll.find_one(dict)
    #     if(d == None):
    #         print("File doesnt exist")
    #         return False
    #     else:
    #         self.coll.delete_one(dict)
    #         return True  

    def exposed_delete_filename(self,path):
        if path == '/':
            self.dire_coll.delete_many({})
            return True

        elif '/' not in path:
            l = []
            dock = self.dire_coll.find_one({path:{'$exists':True}})
            if dock:
                print("hello")
                self.dire_coll.delete_one({path:{'$exists':True}})
                return True
            else: return False

        else:
            dir = path.split('/')
            dock = self.dire_coll.find_one({dir[0]:{'$exists':True}})
            dock2 = dock[dir[0]]
            temp = self.dir_working_for_deleting(dir[1:],dir[-1],dock2,0)
            print("dock : ",dock)
            return temp

    def dir_working_for_deleting(self,dir,endfile,dock2,i):
        print("hello : ",dock2)
        temp = next((obj for obj in dock2 if dir[i] in obj), None)
        # if temp == None:
        print('temp :',temp,"\n endfile : ",endfile,"\ndir : ",dir,"\ndock2: ",dock2,"\ni : ",i)
        if dir[i] == endfile:
            # temp_ = []
            if(temp or temp == []):
                dock2.remove(temp)
                return True
            else:
                # end = {endfile:[]}
                return False
            # dock2.append(end)
            print(f"in dir if not {end}")
            # dock2.append(temp)
            return dock2
        elif temp == None:
            print(f"No directory {dir[i]}")
            return False
        else:
            self.dir_working(dir,endfile,temp[dir[i]],i+1)
            # print(l)
            print("dock2: ",dock2)
            # temp[dir[i]].extend(l)
            print('temp :',temp,"\n endfile : ",endfile,"\ndir : ",dir,"\ndock2: ",dock2,"\ni : ",i)

            print(f"\n\nin dir if there {dock2}\n\n")
            return dock2    
    

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

    def exposed_create_dir(self,path):
        print(path)
        print("/" not in path)
        if ('/' not in path):
            if not self.dire_coll.find_one({path:{ '$exists': True }}):
                if '.' in path:
                    self.dire_coll.insert_one({path :None})
                else:
                    self.dire_coll.insert_one({path :[]})
            else:
                print("Directory exists")
        else:
            dir = path.split('/')
            dock = self.dire_coll.find_one({ dir[0]: { '$exists': True } })
            endfile = dir[-1]
            dock2 = dock[dir[0]]
            print(dock2)

            if dock == None :
                print("No directory in ",dir[0])
            else:
                # pass
                temp = self.dir_working(dir[1:],endfile,dock2,0)
                # print(temp)
                dock[dir[0]] = temp
                # print(f"\n\n{dock}\n\n")
                self.dire_coll.delete_one({ dir[0]:{'$exists':True}})
                self.dire_coll.insert_one(dock)
                # print("We want 10 marks")
                    
                        # else 

    def dir_working(self,dir,endfile,dock2,i):
        print("hello : ",dock2)
        temp = next((obj for obj in dock2 if dir[i] in obj), None)
        # if temp == None:
        print('temp :',temp,"\n endfile : ",endfile,"\ndir : ",dir,"\ndock2: ",dock2,"\ni : ",i)
        if dir[i] == endfile:
            # temp_ = []
            if(temp):
                print(f"file {dir[i]} already exists")
                return
            end = {}
            if '.' in endfile:
                end = {endfile:None}
            else:
                end = {endfile:[]}
            dock2.append(end)
            print(f"in dir if not {end}")
            # dock2.append(temp)
            return dock2
        elif temp == None:
            print(f"No directory {dir[i]}")
        else:
            self.dir_working(dir,endfile,temp[dir[i]],i+1)
            # print(l)
            print("dock2: ",dock2)
            # temp[dir[i]].extend(l)
            print('temp :',temp,"\n endfile : ",endfile,"\ndir : ",dir,"\ndock2: ",dock2,"\ni : ",i)

            print(f"\n\nin dir if there {dock2}\n\n")
            return dock2
                
            






if __name__ == "__main__":
    ip_addr = socket.gethostbyname(socket.gethostname())
    port= 12345
    try:
        print(f'IP-Address : {ip_addr}\nPort :    {port}')
        t = ThreadedServer(NameNode(), port=port)
        t.start()
    except Exception as e:
        print(f"Error starting NameNode server: {e}")
        traceback.print_exc()

