import rpyc
import os
import argparse

class Client:
    def __init__(self, file_path):
        self.conn = rpyc.connect("127.0.0.1", 12345)
        # print("issue")
        self.file_path = file_path
        # self.file_name = file_name

    def send_message(self, message):
        self.conn.root.receive_message(message)

    def send_file_name_to_nn(self):
        self.conn.root.update_filename(self.file_path)
    def send_file_name_to_nnd(self):
        self.conn.root.delete_filename_met(self.file_path)
        a=self.conn.root.delete_filename(self.file_path)
        if a==True:
            print("delete",self.file_path,"successful")
        else:
            print("delete",self.file_path,"unsuccessful,file does not exist in metadata")
    
    def send_file_name_to_nnl(self):
        d=self.conn.root.list_filename(self.file_path)
        if(d == False): print("No directory")
        elif d == True: print("Empty directory")
        else:
            print("Files:")
            for i in d:
                print("\t",i)



    def upload_file(self, path, ip_addr_array,block_size,file_name):
        lis=[]
        print(f"block sie : {block_size}")
        blocks = self.split_file_fixed_size(block_size,file_name)
        print(f"total Blocks {len(blocks)}")
        ip_addr = ip_addr_array['block_locations'][0]
        
        # k = self.upload_block(ip_addr['ip_addr'], ip_addr['port'], path, message, ip_addr_array)

        for i, block in enumerate(blocks):
            k = self.upload_block(ip_addr['ip_addr'], ip_addr['port'], path, block, ip_addr_array)
            lis.append(k)
        # print('\n')
        self.ping_nn_after_upload()
        return lis
        

    def upload_block(self, ip_addr, portno, path, message, ip_addr_array):

        conn = rpyc.connect(ip_addr, portno)
        num = conn.root.receive_message(path, message, ip_addr_array,self.file_path)
        return list(num['data'])

    def split_file_fixed_size(self, block_size,file_name):
        blocks = []

        with open(file_name, 'rb') as file:
            while True:
                data = file.read(block_size)
                if not data:
                    break
                blocks.append(data)
        return blocks

    def get_metadata(self):
        return self.conn.root.write_file()
    
    def ping_nn_after_upload(self):
        self.conn.root.upload_complete()
        print('file upload successful : ',self.file_path)

    def download_file(self, file_name):
        metadata = self.get_metadata()
        # print("metadata:",metadata)
        block_detail = self.get_blockname_data(file_name)
        print("block_detail:",block_detail)
        if not metadata or "block_locations" not in metadata:
            print(f"File '{file_name}' not found or metadata is missing.")
            return
        if(metadata['block_locations']==None or metadata['block_locations']==[]):
            print("no datanode is active and hence can't retrieve")
            return
           
        if(not block_detail):
            print("file doesn't exist")
            return
        # Step 2: Retrieve block locations
        block_locations = metadata["block_locations"]

        # Step 3: Retrieve data blocks from DataNodes
        file_data = b""
        for block_location in block_locations:
            working_datanode = False
            ip_addr = block_location["ip_addr"]
            port_no = block_location["port"]
            # block_name = block_location["block_name"]
            # block_name = 
            # Connect to the DataNode
            data_node_conn = rpyc.connect(ip_addr, port_no)
            block_index = 1
            # Request the data block from the DataNode
            while True:
                # block_name = 
                if str(block_index) in block_detail:
                    try:
                        block_name = block_detail[str(block_index)][0]
                        data_block = data_node_conn.root.retrieve_data_block(block_name)
                        # print('block : ',data_block)
                        ## IF we failed to store data in block
                        if data_block=="":
                            block_name = block_detail[str(block_index)][1]
                            data_block = data_node_conn.root.retrieve_data_block(block_name)
                            if data_block=="":
                                block_name = block_detail[str(block_index)][2]
                                data_block = data_node_conn.root.retrieve_data_block(block_name)                            
                    except:
                            try:
                                block_name = block_detail[str(block_index)][1]
                                data_block = data_node_conn.root.retrieve_data_block(block_name) 
                                if data_block=="":
                                    block_name = block_detail[str(block_index)][1]
                                    data_block = data_node_conn.root.retrieve_data_block(block_name)
                            except:
                                try:
                                    block_name = block_detail[str(block_index)][2]
                                    data_block = data_node_conn.root.retrieve_data_block(block_name)
                                    if data_block=="":
                                        print("Empty nothing to see here")
                                except:
                                    print("Error you are datanodes are dead or file is corrupted/missing")
                                    break                                                            
                    file_data += data_block
                    block_index+=1
                    # block_name = str(block_index)

                else:
                    print('file not there')
                    break
            data_node_conn.close()
            # print('filedata : ',file_data)
            break

            # Close the connection to the DataNode

        # Step 4: Reassemble the file
        if(file_data==b''):
            print("either the datanode doesnt have file or the file is empty")
        else :
            dile = file_name.split('/')[-1]
            file_path = os.path.join(os.getcwd(), "downloads", dile)
            with open(file_path, "wb") as output_file:
                output_file.write(file_data)

            print(f"File '{dile}' downloaded successfully.")

    def get_data_nodes(self):
        return self.conn.root.write_file()

    def get_blockname_data(self,filename):
        dock = self.conn.root.get_blockname_data(filename)
        return dock
    
    def create_directory(self):
        self.conn.root.create_dir(self.file_path)

    def close_connection(self):
        self.conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for uploading and downloading files from a distributed file system.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    put_parser = subparsers.add_parser("put", help="Upload a file")
    put_parser.add_argument("file_name", help="Name of the file to upload")
    put_parser.add_argument("file_path",help = "Path to the file")

    get_parser = subparsers.add_parser("get", help="Download a file")
    get_parser.add_argument("file_name", help="Name of the file to download")
    
    del_parser = subparsers.add_parser("del", help="Delete a file")
    del_parser.add_argument("file_name", help="Name of the file to delete")

    l_parser = subparsers.add_parser("list", help="List a file")
    l_parser.add_argument("file_name", help="Name of the file to delete")

    mkdir_parser = subparsers.add_parser("mkdir", help="Delete a file")
    mkdir_parser.add_argument("file_name", help="Name of the file to delete")

    args = parser.parse_args()
    #print(args)
    if args.command == "put":
        client = Client(args.file_path)
        block_size= 500  # Adjust to your preferred fixed block size
        lis = client.get_data_nodes()
        print(len(lis['block_locations']))
        client.send_file_name_to_nn()
        client.create_directory()
        flag = client.upload_file("one", lis,block_size,args.file_name)

        print("Info:",flag)
        client.close_connection()
    elif args.command == "get":
        client = Client("")
        client.download_file(args.file_name)
        client.close_connection()
    elif args.command == "del":
        client = Client(args.file_name)
        client.send_file_name_to_nnd()
        client.close_connection()
    elif args.command=='list':
        client=Client(args.file_name)
        client.send_file_name_to_nnl()
        client.close_connection()

    elif args.command == 'mkdir':
        client = Client(args.file_name)
        client.create_directory()
        client.close_connection()

