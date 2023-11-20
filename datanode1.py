import rpyc
import threading
import os
from datetime import datetime
import socket

count = 50

class DataNode(rpyc.Service):
    def __init__(self,nn_details):
        self.nn_ip_addr = nn_details['ip_addr']
        self.nn_port = nn_details['port']

    def on_connect(self, conn):
        self.client_conn = conn
        print("DataNode1 connected")

    def on_disconnect(self, conn):
        print("DataNode1 disconnected")

    def connect(self, ip_addr, portno, path, message, lis,extension,filename):
        self.conn = rpyc.connect(ip_addr, portno)
        num = self.conn.root.ripple(path, message,extension,filename)
        # self.conn.close()
        lis.append(num['val'])

    def exposed_receive_message(self, path, message, ip_addr_array,filepath):
        print('In datanode2')
        extension = filepath.split('.')[-1]
        filename = filepath.split('/')[-1]
        lis = []
        index = 1
        index=index%len(ip_addr_array['block_locations'])
        paths = self.create_directory(path)
        global count
        # lis.append(count)
        asd = str(count)
        count += 1
        formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = formatted_time+ '_' + filename + '_' + asd + '.' + extension
        lis.append(file_name)
        file_path = os.path.join(paths, file_name)

        with open(file_path, "wb") as output_file:
            output_file.write(message)
        # print(ip_addr_array)
        ip_data = ip_addr_array['block_locations']
        thread1 = threading.Thread(target=self.connect, args=(ip_data[index]['ip_addr'],ip_data[index]['port'], path, message, lis,extension,filename))
        index+=1
        index=index%len(ip_addr_array['block_locations'])
        thread2 = threading.Thread(target=self.connect, args=(ip_data[index]['ip_addr'] ,ip_data[index]['port'], path, message, lis,extension,filename))
        thread1.start()
        thread1.join()
        thread2.start()
        thread2.join()
        self.send_block_names_to_namenode(lis)
        data = {'data':lis}
        # print(data)
        return data

    def send_block_names_to_namenode(self,lis):
        nn_conn = rpyc.connect(self.nn_ip_addr,self.nn_port)
        data = {'data':lis}
        nn_conn.root.update_block_names(data)
        nn_conn.close()

    def exposed_send_message_to_client(self):
        message = []
        return message  # Return the message to the client

    def exposed_retrieve_data_block(self, block_name):
        # Implement logic to retrieve and return the data block
        # block_name = str(block_name)+'.txt'
        block_path = os.path.join(os.getcwd(), "datanode2\\one", block_name)
        with open(block_path, "rb") as block_file:
            return block_file.read()

    def create_directory(self, path):
        current_directory = os.getcwd()
        full = os.path.join(current_directory, "datanode2")

        full_path = os.path.join(full, path)
        if not os.path.exists(full_path):
            os.makedirs(full_path)
        return full_path

    def connect_to_namenode(self,ip_addr,port):
        print(f'nn: ip:{self.nn_ip_addr},port : {self.nn_port}')
        nn_conn = rpyc.connect(self.nn_ip_addr,self.nn_port)
        nn_conn.root.mark_datanode(ip_addr,port )
        nn_conn.close()

    # Myyyyyy changessssss
    def exposed_ripple(self, path, message,extension,filename):
        global count
        asd = str(count)
        count += 1
        formatted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = formatted_time+ '_' + filename + '_' + asd + '.' + extension
        # file_name = asd + '.txt'
        path = self.create_directory(path)
        file_path = os.path.join(path, file_name)

        with open(file_path, "wb") as output_file:
            output_file.write(message)

        return {'val':file_name}

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    port = 12347
    nn_details = {'port':12345,'ip_addr':'localhost'}
    dn_obj = DataNode(nn_details)
    ip_addr = socket.gethostbyname(socket.gethostname())
    dn_obj.connect_to_namenode(ip_addr,port)
    t = ThreadedServer(dn_obj, port=12347)
    t.start()

