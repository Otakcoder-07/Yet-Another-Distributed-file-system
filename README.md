# Yet Another Distributed File System (YADFS)

YADFS is a distributed file system inspired by Google File System (GFS), built using Python and leveraging MongoDB for metadata storage. It is designed to provide a fault-tolerant, highly available, and scalable storage solution for handling large volumes of data across multiple nodes.

## Features

### 1. Replication
YADFS employs a replication mechanism to ensure data durability and availability. Each file is replicated across multiple data nodes, providing fault tolerance and preventing data loss in the event of a node failure.

### 2. Dynamic Leadership
YADFS allows any data node to act as a leader, enhancing fault tolerance. In case of a leader node failure, another data node can seamlessly take over the leadership role to ensure continuous operation.

### 3. Heartbeat Mechanism
To maintain an up-to-date view of the system's health, each data node sends periodic heartbeat signals to the central NameNode. This mechanism allows the system to detect and react to node failures promptly.

### 4. Fault Tolerance
YADFS is designed to handle interruptions or failures gracefully. Tasks interrupted due to node failures are automatically restarted, ensuring the system's continuous operation and data consistency.

### 5. Metadata Storage with MongoDB
MongoDB is utilized for storing metadata, providing a robust and scalable solution for managing file system metadata. This choice enhances the system's performance and reliability.

### 6. Directory Operations
YADFS supports the creation of directories and nested directories, allowing users to organize their data effectively. Additionally, users can add, delete, and manage files within the distributed file system.

### 7. Single Node Resilience
Even if one data node is operational, YADFS can continue to function. The system's design ensures that data remains accessible and tasks can be completed, even in the presence of node failures.


