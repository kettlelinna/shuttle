# Shuttle: High Available, High Performance Remote Shuffle Service
RSS provides remote shuffle capability to group and dump shuffle data into distribute file system by partition. 

The goal of RSS is to transfer the small and random IO to sequence IO, result to improve the performance and  stablility of application.
See more details on [docs link](docs/server-high-level-design.md).

Our email: bigdata-arch@oppo.com

Please contact us if you has any question or suggestion.
## Spark Version matching

| branch name | spark version |
| ----------- | ------------- |
| sp24 | spark2.4.x |
| sp31 | >=spark3.0 |
| master | >=spark3.0 |

RSS supports AQE of spark 2.x.  
RSS supports AQE of spark3.x except local read. Therefore, in spark3.x, you need to add the following configuration to close AQE local reading:
```
spark.sql.adaptive.localShuffleReader.enabled=false
```

## Build Guide
Use JDK 8+ and maven 3+ to build

## Build RSS servers distribute [ shuffle  master/worker  ]
`
sh build/build.sh
`

This command generates dist/rss-spark.zip. The directory structure :
`
conf bin lib client
`

## Build rss servers [ shuffle master/worker ]
`
mvn clean package -Pserver -DskipTests
`

The jars generated by the command: 
      rss-xxx-master.jar for rss Shuffle Master
rss-xxx-server.jar  for rss Shuffle Worker

## Build rss clients [ shuffle manager/writer/reader ]
`
mvn clean package -Pclient -DskipTests
`

This command generates clients jars:
rss-xxx-client.jar   for rss Shuffle Clients

## Other Server Depends on Zookeeper、Distribute File System [ HDFS / CFS / Alluxio ]

## Necessary configuration
Modify the following options in conf/rss_env.sh：
```
# Define rss cluster name
export RSS_DATA_CENTER=dc1
export RSS_CLUSTER=cluster1

#The root directory where shuffle data is saved
export RSS_ROOT_DIR=hdfs://user/rss-data

export RSS_ZK_SERVERS=10.10.10.1:2181
```

if use HDFS, add core-site.xml and hdfs-site.xml to the conf directory.

if use CFS,  add cfs-site.xml to the conf directory.

if use Alluxio, add alluxio-site.xml to the conf directory

## How to Run
### ShuffleMaster
Start ShuffleMaster server with distribute package as a java application, run:

`
sh bin/run_master.sh start
`

ShuffleMaster is a HA service,  you can start master on other machines.

### ShuffleWorker
Start ShuffleWorker server with distribute package as a java application, run:

`
sh bin/run_worker.sh start
`

Remarks:
ShuffleWorker uses port to specification, so we can start more shuffle workers with diff ports on one host.

### ShuffleClients
The client side does not need to add the configuration of hdfs, cfs, etc. on the server side, and the client will automatically obtain these configurations from the shuffle master.
### Static resource allocation
1、Deploy rss-xxx-client.jar to hdfs. Then add configure to your Spark application like following (you need to adjust the values based on your environment):
```
spark.dynamicAllocation.enabled                       false
spark.shuffle.service.enabled                         false
spark.executor.extraClassPath                         rss-xxx-client.jar
spark.shuffle.manager                                 org.apache.spark.shuffle.Ors2ShuffleManager
spark.shuffle.rss.serviceManager.type                 master
spark.shuffle.rss.serviceRegistry.zookeeper.servers  10.10.10.1:2181
spark.shuffle.rss.dataCenter                         dc1
spark.shuffle.rss.cluster                            cluster1
```
2、Run your Spark application

This is convenient for you to quickly test rss, and it is not recommended to use it in a production environment

### Dynamic resource allocation
Spark dynamic resource allocation to support remote shuffle service requires modification of spark-core source code and recompilation.  See more details on Spark community document: [[SPARK-25299][DISCUSSION] Improving Spark Shuffle Reliability](https://docs.google.com/document/d/1uCkzGGVG17oGC6BJ75TpzLAZNorvrAU3FRd2X-rVHSM/edit?ts=5e3c57b8).

In the source code, you can find patch modified by spark 2.x and 3.x to spark-core. Merge the patch and compile spark, and then replace spark-core-xx.jar.

Change configure to your Spark application:
```
spark.dynamicAllocation.enabled                       true
spark.shuffle.service.enabled                         true
```
The adaptation is perfect and will not connect to the port of yarn external shuffle.

## Configuration
### Environment variable

| name | describe |
| :----: | :--------: |
|RSS_CONF_DIR	| Set the server configuration file directory, default "$PWD/conf" |
|RSS_DATA_CENTER |Service data center name | 
|RSS_CLUSTER |	Service data cluster name |
|RSS_ROOT_DIR | Shuffle data is written to the root directory |
|RSS_REGISTER_TYPE	| Shuffle worker registration type, default "master" |
|RSS_MASTER_NAME | Set the shuffle master service name |
|RSS_ZK_SERVERS | Zookeeper address for service registration and management |
|RSS_MASTER_JVM_OPTS | master jvm options |
|RSS_WORKER_JVM_OPTS | worker jvm options |
|RSS_MASTER_MEMORY	| Set the master jvm memory, for example: "-Xms2g -Xmx2g" |
|RSS_WORKER_MEMORY	| Set the worker jvm memory, for example: "-Xms6g -Xmx6g" |
|RSS_MASTER_SERVER_OPTS | Set master startup parameters, for example: "-masterPort 19189" |
|RSS_WORKER_SERVER_OPTS | Set worker startup parameters, for example: "-buildConnectionPort 19191 -port 19190" |

### Master startup parameters

| name | describe |
| :----: | :--------: |
|masterName | Set master name |
|masterPort	| shuffle master rpc port |
|httpPort	| shuffle master http port |
|zooKeeperServers	| Service registration management zookeeper address |
|dataCenter	| The master assigns the worker's data center name by default |
|cluster	| The master assigns the worker's data clustername by default |

### Worker startup parameters

| name | describe |
| :----: | :--------: | 
|serviceRegistry | Set the shuffle worker registration type, the default master |
|masterName	| Set which master the worker service is registered to |
|zooKeeperServers | Service registration management zookeeper address |
|dataCenter	| Set the name of the data center where the worker is registered |
|cluster	| Set the name of the cluster name where the worker is registered |
|workerLoadWeight	| Set the worker distribution weight. This is typically used for heterogeneous clusters and defaults to 1 |
|rootDir	| The root directory where shuffle data is written |
|buildConnectionPort	| Connect the control port |
|port	| Data transfer port |
|dumperThreads	| The number of write data threads, the default is the number of cpu cores |
|dumperQueueSize	| Write the maximum size of each thread queue, default 100 |
|nettyWorkerThreads	| The number of netty threads, the default is 16 |
|memoryControlSizeThreshold	| The maximum size of memory used by shuffle data, the default is half of the jvm memory |
|baseConnections	| Number of tokens for flow control basis |
|totalConnections	| Flow control maximum number of tokens |
|appStorageRetentionMillis	| The maximum survival time of shuffle data, the default is 24h. If it exceeds this time, it will be automatically deleted. |
|appObjRetentionMillis	| The maximum survival time of the app, stage and other data saved in the shuffle worker memory, the default is 6h |

### Spark client
| name | describe |
| :----: | :--------: | 
|spark.shuffle.rss.masterName	| Set the master name, the address of the master will be obtained from zookeeper according to this name |
|spark.shuffle.rss.serviceManager.type	| Set the shuffle worker management registration method, the default master |
|spark.shuffle.rss.serviceRegistry.zookeeper.servers	| Service registration management zookeeper address |
|spark.shuffle.rss.writer.blockSize	| Shuffle write data block size, default 1mb |
|spark.shuffle.rss.writer.maxRequestSize	| The maximum size of a request for network transmission, the default is 2mb |
|spark.shuffle.rss.writer.maxFlyingPackageNum	| The maximum number of outstanding network requests allowed, the default is 16. In the case of network latency, this effectively controls memory usage |
|spark.shuffle.rss.memory.threshold	| When shuffle write is in unsafe mode, the maximum amount of data that can be buffered in off-heap memory is 512MB by default. Its role is to reduce network transmission latency and balance network IO. |
|spark.shuffle.rss.network.ioThreads	| When shuffle write, the number of network transmission threads, the default is 2 |
|spark.shuffle.rss.writer.bufferSpill	| During the shuffle write process, the maximum buffer data size in memory, the default is 128mb |
|spark.shuffle.rss.writer.type	| Set the shuffle write type, including auto, bypass, unsafe, and sort. The default is auto, a shuffle write type will be automatically selected, and its selection logic is basically the same as spark sort shuffle |
|spark.shuffle.rss.partitionCountPerShuffleWorker	| How many partitions each shuffle worker can allocate, defaults to 5. |
|spark.shuffle.rss.read.io.threads	| The number of shuffle read threads, the default is 2 |
|spark.shuffle.rss.read.max.size	| In the shuffle read process, the maximum amount of data buffered in memory, the default is 128m. This will effectively reduce the probability of oom in the shuffle read process |
|spark.shuffle.rss.read.merge.size	| After setting how much data the network reads, the data will be packaged and added to the shuffle read pending data queue. |
|spark.shuffle.rss.deleteShuffleDir	| Whether to delete the shuffle data directory automatically after the task ends. Defaults to true |
