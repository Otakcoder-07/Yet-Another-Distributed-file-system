import rpyc
import os
import argparse

class Client:
    def __init__(self, file_path):
        self.conn = rpyc.connect("192.168.56.1", 12345)
        # print("issue")
        self.file_path = file_path

    def send_message(self, message):
        self.conn.root.receive_message(message)

    def send_file_name_to_nn(self):
        self.conn.root.update_filename(self.file_path)

    def upload_file(self, path, ip_addr_array,block_size):
        lis=[]
        print(block_size)
        blocks = self.split_file_fixed_size(block_size)
        print(f"total Blocks {len(blocks)}")
        ip_addr = ip_addr_array['block_locations'][0]
        
        # k = self.upload_block(ip_addr['ip_addr'], ip_addr['port'], path, message, ip_addr_array)

        for i, block in enumerate(blocks):
            k = self.upload_block(ip_addr['ip_addr'], ip_addr['port'], path, block, ip_addr_array)
            lis.append(k)
        print('\n\n\n\n\n\n')
        self.ping_nn_after_upload()
        return lis
        

    def upload_block(self, ip_addr, portno, path, message, ip_addr_array):

        conn = rpyc.connect(ip_addr, portno)
        num = conn.root.receive_message(path, message, ip_addr_array,self.file_path)
        return list(num['data'])

    def split_file_fixed_size(self, block_size):
        blocks = []

        with open(self.file_path, 'rb') as file:
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
        print('\n\ndone\n\n')

    def download_file(self, file_name):
        metadata = self.get_metadata()
        print("metadata:",metadata)
        block_detail = self.get_blockname_data(file_name)
        print("block_detail:",block_detail)
        if not metadata or "block_locations" not in metadata:
            print(f"File '{file_name}' not found or metadata is missing.")
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
                    block_name = block_detail[str(block_index)][0]

                    data_block = data_node_conn.root.retrieve_data_block(block_name)
                    print('block : ',data_block)
                    file_data += data_block
                    block_index+=1
                    # block_name = str(block_index)

                else:
                    print('not there')
                    break
            data_node_conn.close()
            # print('filedata : ',file_data)
            break

            # Close the connection to the DataNode

        # Step 4: Reassemble the file
        file_path = os.path.join(os.getcwd(), "downloads", file_name)
        with open(file_path, "wb") as output_file:
            output_file.write(file_data)

        print(f"File '{file_name}' downloaded successfully.")

    def get_data_nodes(self):
        return self.conn.root.write_file()

    def get_blockname_data(self,filename):
        dock = self.conn.root.get_blockname_data(filename)
        return dock
    def close_connection(self):
        self.conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client for uploading and downloading files from a distributed file system.")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    put_parser = subparsers.add_parser("put", help="Upload a file")
    put_parser.add_argument("file_name", help="Name of the file to upload")

    get_parser = subparsers.add_parser("get", help="Download a file")
    get_parser.add_argument("file_name", help="Name of the file to download")
    
    del_parser = subparsers.add_parser("del", help="Delete a file")
    del_parser.add_argument("file_name", help="Name of the file to delete")

    args = parser.parse_args()

    if args.command == "put":
        client = Client(args.file_name)
        block_size= 500  # Adjust to your preferred fixed block size
        lis = client.get_data_nodes()
        print(len(lis['block_locations']))
        client.send_file_name_to_nn()
        
        client.upload_file("one", lis,block_size)

        client.close_connection()
    elif args.command == "get":
        client = Client("")
        client.download_file(args.file_name)
        client.close_connection()
    elif args.command == "del":
        client = Client(args.file_name)
        client.send_file_name_to_nnd()
        client.close_connection()
