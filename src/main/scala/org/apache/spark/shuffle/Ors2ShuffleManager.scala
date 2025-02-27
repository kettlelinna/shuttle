/*
 * Copyright 2021 OPPO. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle

import com.oppo.shuttle.rss.BuildVersion
import com.oppo.shuttle.rss.clients.Ors2ShuffleClientFactory
import com.oppo.shuttle.rss.common.{AppTaskInfo, Constants, Ors2ServerGroup, Ors2WorkerDetail, StageShuffleInfo}
import com.oppo.shuttle.rss.exceptions.{Ors2Exception, Ors2NoShuffleWorkersException}
import com.oppo.shuttle.rss.metadata.{Ors2MasterServerManager, ServiceManager, ZkShuffleServiceManager}
import com.oppo.shuttle.rss.util.ShuffleUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulerUtils
import org.apache.spark.shuffle.ors2.{Ors2BlockManager, Ors2ClusterConf, Ors2ShuffleReadMetrics, Ors2ShuffleWriteMetrics, Ors2SparkListener}
import org.apache.spark.shuffle.sort.{Ors2UnsafeShuffleWriter, SortShuffleManager, SortShuffleWriter}
import org.apache.spark.sql.internal.SQLConf.{ADAPTIVE_EXECUTION_ENABLED, LOCAL_SHUFFLE_READER_ENABLED}

import scala.collection.JavaConverters._
import scala.util.{Random => SRandom}


class Ors2ShuffleManager(sparkConf: SparkConf) extends ShuffleManager with Logging {
  logInfo(s"Creating ShuffleManager instance: ${this.getClass.getSimpleName}, project version: ${BuildVersion.projectVersion}," +
    s" git commit revision: ${BuildVersion.gitCommitVersion}")

  private val SparkYarnQueueConfigKey = "spark.yarn.queue"
  private val SparkAppNameKey = "spark.app.name"

  private val networkTimeoutMillis = sparkConf.get(Ors2Config.networkTimeout).toInt
  private val networkRetries = sparkConf.get(Ors2Config.getClientMaxRetries)
  private val retryInterval = sparkConf.get(Ors2Config.ioRetryWait)
  private val inputReadyQueryInterval = sparkConf.get(Ors2Config.dataInputReadyQueryInterval)
  private val inputReadyMaxWaitTime = sparkConf.get(Ors2Config.shuffleReadWaitFinalizeTimeout)

  private val dataCenter = sparkConf.get(Ors2Config.dataCenter)
  private val cluster = sparkConf.get(Ors2Config.cluster)
  private val useEpoll = sparkConf.get(Ors2Config.useEpoll)
  private var masterName = sparkConf.get(Ors2Config.masterName)
  private val isGetActiveMasterFromZk: Boolean = sparkConf.get(Ors2Config.isGetActiveMasterFromZk)
  private val dagId = sparkConf.get(Ors2Config.dagId)
  private val jobPriority = sparkConf.get(Ors2Config.jobPriority)
  private val taskId = sparkConf.get(Ors2Config.taskId)
  private val appName = sparkConf.get(SparkAppNameKey, "")
  private val dfsDirPrefix: String = sparkConf.get(Ors2Config.dfsDirPrefix)

  private var serviceManager: ServiceManager = _

  private val shuffleClientFactory: Ors2ShuffleClientFactory = new Ors2ShuffleClientFactory(sparkConf)

  private def getSparkContext = {
    SparkContext.getActive.get
  }

  /**
   * Called by Spark app driver.
   * Fetch Ors2 shuffle workers to use.
   * Return a ShuffleHandle to driver for (getWriter/getReader).
   */
  override def registerShuffle[K, V, C](shuffleId: Int, dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    logInfo(s"Use ShuffleManager: ${this.getClass.getSimpleName}")

    if (sparkConf.get(ADAPTIVE_EXECUTION_ENABLED) && sparkConf.get(LOCAL_SHUFFLE_READER_ENABLED)) {
      throw new Ors2Exception(s"shuttle rss does not support local file reading. " +
        s"Please set ${LOCAL_SHUFFLE_READER_ENABLED.key} to false")
    }

    if (sparkConf.get("spark.dynamicAllocation.enabled", "false").toBoolean) {
      throw new Ors2Exception(s"shuttle rss does not support dynamicAllocation. " +
        s"Please set spark.dynamicAllocation.enabled to false")
    }

    val blockSize = sparkConf.get(Ors2Config.writeBlockSize)
    val minBlockSize = sparkConf.get(Ors2Config.minWriteBlockSize)
    val maxBlockSize = sparkConf.get(Ors2Config.maxWriteBlockSize)
    if (blockSize < minBlockSize || blockSize > maxBlockSize) {
      throw new RuntimeException(s"config ${Ors2Config.writeBlockSize.key} must be between ${minBlockSize} and ${maxBlockSize}")
    }

    val numPartitions = dependency.partitioner.numPartitions
    val sparkContext = getSparkContext
    val user = sparkContext.sparkUser
    val queue = sparkConf.get(SparkYarnQueueConfigKey, "")

    val appId = sparkConf.getAppId
    val appAttemptId = sparkContext.applicationAttemptId.getOrElse("0")

    val (clusterConf: Ors2ClusterConf, shuffleWorkers: Array[Ors2WorkerDetail]) = getShuffleWorkers(numPartitions)
    val (partitionMapToShuffleWorkers: Map[Int, Int], ors2ServerHandles: Array[Ors2ShuffleServerHandle]) = distributeShuffleWorkersToPartition(shuffleId, numPartitions, shuffleWorkers)

    logInfo(s"partitionMapToShuffleWorkers to shuffle id $shuffleId size: ${partitionMapToShuffleWorkers.size}: $partitionMapToShuffleWorkers")

    // write flag file to dfs when stage is completed or app is completed, and delete the flag file when stage is rerun
    Ors2SparkListener.registerListener(sparkContext, sparkConf.getAppId, shuffleId, shuffleWorkers, networkTimeoutMillis, clusterConf, getOrCreateServiceManager)

    val dependencyInfo = s"numPartitions: ${dependency.partitioner.numPartitions}, " +
      s"serializer: ${dependency.serializer.getClass.getSimpleName}, " +
      s"keyOrdering: ${dependency.keyOrdering}, " +
      s"aggregator: ${dependency.aggregator}, " +
      s"mapSideCombine: ${dependency.mapSideCombine}, " +
      s"keyClassName: ${dependency.keyClassName}, " +
      s"valueClassName: ${dependency.valueClassName}"

    logInfo(s"RegisterShuffle: $appId, $appAttemptId, $shuffleId, $dependencyInfo")

    new Ors2ShuffleHandle(
      user,
      queue,
      appId,
      appAttemptId,
      dependency,
      shuffleId,
      partitionMapToShuffleWorkers,
      ors2ServerHandles,
      clusterConf
    )
  }

  /**
   * Called by Spark app executors, get ShuffleWriter from Spark driver via the ShuffleHandle.
   */
  override def getWriter[K, V](
                                h: ShuffleHandle,
                                mapId: Long,
                                taskContext: TaskContext,
                                metrics: ShuffleWriteMetricsReporter
                              ): ShuffleWriter[K, V] = {
    h match {
      case shuffleHandle: Ors2ShuffleHandle[K@unchecked, V@unchecked, _] =>
        val appTaskInfo = new AppTaskInfo(
          sparkConf.getAppId,
          shuffleHandle.appAttemptId,
          shuffleHandle.partitionMapToShuffleWorkers.size,
          sparkConf.get(Ors2Config.partitionCountPerShuffleWorker),
          shuffleHandle.shuffleId,
          taskContext.partitionId(),
          taskContext.attemptNumber(),
          taskContext.stageAttemptNumber()
        )

        val blockManager = Ors2BlockManager(
          taskContext = taskContext,
          numPartitions = shuffleHandle.dependency.partitioner.numPartitions,
          partitionMapToShuffleWorkers = shuffleHandle.partitionMapToShuffleWorkers,
          appTaskInfo = appTaskInfo,
          serverGroups = shuffleHandle.getAllServerGroups,
          Ors2ShuffleWriteMetrics(metrics),
          sparkConf.get(Ors2Config.writerBufferSpill).toInt,
          shuffleClientFactory
        )

        var writeType = sparkConf.get(Ors2Config.shuffleWriterType)
        if (writeType == Ors2Config.SHUFFLE_WRITER_AUTO) {
          if (SortShuffleWriter.shouldBypassMergeSort(sparkConf, shuffleHandle.dependency)) {
            writeType = Ors2Config.SHUFFLE_WRITER_BYPASS
          } else if (SortShuffleManager.canUseSerializedShuffle(shuffleHandle.dependency)) {
            writeType = Ors2Config.SHUFFLE_WRITER_UNSAFE
          } else {
            writeType = Ors2Config.SHUFFLE_WRITER_SORT
          }
        }

        val writer: ShuffleWriter[K, V] = writeType match {
          case Ors2Config.SHUFFLE_WRITER_BYPASS =>
            Ors2BypassShuffleWriter(
              blockManager,
              shuffleHandle.dependency,
              taskContext,
              sparkConf
            )
          case Ors2Config.SHUFFLE_WRITER_UNSAFE =>
            new Ors2UnsafeShuffleWriter(
              blockManager,
              shuffleHandle.dependency,
              taskContext,
              sparkConf
            )
          case Ors2Config.SHUFFLE_WRITER_SORT =>
            Ors2SortShuffleWriter(
              blockManager,
              shuffleHandle.dependency,
              taskContext,
              sparkConf
            )
          case _ => throw new RuntimeException(s"not support ${Ors2Config.shuffleWriterType}: $writeType")
        }

        val numPartitions = shuffleHandle.dependency.partitioner.numPartitions
        logInfo(s"shuttle rss writer use ${writer.getClass.getSimpleName}. " +
          s"$shuffleHandle, numPartitions: $numPartitions, mapId: $mapId, stageId: ${taskContext.stageId()}, shuffleId: ${shuffleHandle.shuffleId}")
        writer
      case _ => throw new RuntimeException(s"not support handle: $h")
    }
  }

  /**
   * Called by Spark executors, get ShuffleReader from Spark driver via the ShuffleHandle.
   */
  override def getReader[K, C](
                                handle: ShuffleHandle,
                                startMapIndex: Int,
                                endMapIndex: Int,
                                startPartition: Int,
                                endPartition: Int,
                                context: TaskContext,
                                metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    getReaderForRange(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  def getReaderForRange[K, C](
                               handle: ShuffleHandle,
                               startMapIndex: Int,
                               endMapIndex: Int,
                               startPartition: Int,
                               endPartition: Int,
                               taskContext: TaskContext,
                               metrics: ShuffleReadMetricsReporter
                             ): ShuffleReader[K, C] = {
    logInfo(s"shuttle rss getReaderForRange: Use ShuffleManager: ${this.getClass.getSimpleName}, $handle, partitions: [$startPartition, $endPartition)")

    val ors2ShuffleHandle = handle.asInstanceOf[Ors2ShuffleHandle[K, _, C]]
    val stageShuffleInfo = new StageShuffleInfo(
      sparkConf.getAppId,
      ors2ShuffleHandle.appAttemptId,
      taskContext.stageAttemptNumber(),
      handle.shuffleId
    )

    val serializer = ors2ShuffleHandle.dependency.serializer

    new Ors2ShuffleReader(
      user = ors2ShuffleHandle.user,
      clusterConf = ors2ShuffleHandle.clusterConf,
      stageShuffleInfo = stageShuffleInfo,
      startMapIndex = startMapIndex,
      endMapIndex = endMapIndex,
      startPartition = startPartition,
      endPartition = endPartition,
      serializer = serializer,
      taskContext = taskContext,
      sparkConf = sparkConf,
      shuffleDependency = ors2ShuffleHandle.dependency,
      inputReadyCheckInterval = inputReadyQueryInterval,
      inputReadyWaitTime = inputReadyMaxWaitTime,
      shuffleMetrics = Ors2ShuffleReadMetrics(metrics)
    )
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    true
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    new Ors2ShuffleBlockResolver()
  }

  override def stop(): Unit = {
    try {
      if (serviceManager != null) {
        serviceManager.close()
      }
      shuffleClientFactory.stop()
    } catch {
      case e: Throwable => log.error("Stop error", e)
    }
  }

  private def getOrCreateServiceManager: ServiceManager = {
    if (serviceManager != null) {
      return serviceManager
    }

    val zkManager = new ZkShuffleServiceManager(getZooKeeperServers, networkTimeoutMillis, networkRetries)

    val serviceManagerType = sparkConf.get(Ors2Config.serviceManagerType)
    serviceManager = serviceManagerType match {
      case Constants.MANAGER_TYPE_ZK =>
        zkManager
      case Constants.MANAGER_TYPE_MASTER =>
        initActiveMaster(zkManager)
        new Ors2MasterServerManager(zkManager, networkTimeoutMillis, retryInterval, masterName, useEpoll)
      case _ => throw new RuntimeException(s"Invalid service registry type: $serviceManagerType")
    }

    serviceManager
  }

  private def getZooKeeperServers: String = {
    var serversValue = sparkConf.get(Ors2Config.serviceRegistryZKServers)
    // Translation compatible with old configuration
    if (StringUtils.isEmpty(serversValue)) {
      serversValue = sparkConf.get("spark.shuffle.ors2.serviceRegistry.zookeeper.servers")
    }
    serversValue
  }

  private def initActiveMaster(zkManager: ZkShuffleServiceManager): Unit = {
    if (isGetActiveMasterFromZk) {
      // Try to get the currently used master from zk
      val zkActiveMaster = zkManager.getActiveCluster
      if (zkActiveMaster != null) {
        masterName = zkActiveMaster
      } else {
        log.warn("Active master name is not set in zk path /shuffle_rss_path/use_cluster/shuffle_master, " +
          "so use the default master name")
      }
    }
    logInfo(s"shuttle rss use master $masterName, isGetActiveMasterFromZk $isGetActiveMasterFromZk")
  }

  def randomItem[T](items: Array[T]): T = {
    items(SRandom.nextInt(items.length))
  }

  protected[spark] def distributeShuffleWorkersToPartition(
                                                            shuffleId: Int,
                                                            numPartitions: Int,
                                                            workers: Array[Ors2WorkerDetail]): (Map[Int, Int], Array[Ors2ShuffleServerHandle]) = {
    val workersPerGroup = SchedulerUtils.getWorkerGroupSize(shuffleId)

    val serverDetails = SRandom.shuffle(workers.toList)

    val serverGroup = serverDetails.indices.map(id => {
      id.until(workersPerGroup + id).map(idx => {
        val i = idx % serverDetails.length
        serverDetails(i)
      }).distinct.toList
    }).toArray

    if (serverGroup.isEmpty) {
      throw new Ors2NoShuffleWorkersException("There is no reachable shuttle rss server")
    }
    logInfo(s"shuttle rss server assign to shuffle id $shuffleId group size: ${serverGroup.length}," +
      s" workersPerGroup: ${workersPerGroup}, serverCombinations:")

    serverGroup.indices.foreach(id => {
      logInfo(s"ShuffleWorkerGroupIdx: $id, size: ${serverGroup(id).length},  workers: ${serverGroup(id)}")
    })

    val shuffleHandles = serverGroup
      .map(group => Ors2ShuffleServerHandle(new Ors2ServerGroup(group.asJava)))

    // Workers used by the partition, evenly distributed
    val partitionMapToShuffleWorkers = 0.until(numPartitions).map(part => {
      (part, part % serverGroup.length)
    }).toMap

    (partitionMapToShuffleWorkers, shuffleHandles)
  }

  /**
   * Fetch Ors2 shuffleWorkers, from zk or shuffleMaster
   *
   * @param numPartitions
   * @return
   */
  private def getShuffleWorkers(numPartitions: Int): (Ors2ClusterConf, Array[Ors2WorkerDetail]) = {
    logInfo(s"getShuffleWorkers numPartitions: $numPartitions")
    val maxServerCount = sparkConf.get(Ors2Config.maxRequestShuffleWorkerCount)
    val minServerCount = sparkConf.get(Ors2Config.minRequestShuffleWorkerCount)
    val partitionCountPerShuffleWorker = sparkConf.get(Ors2Config.partitionCountPerShuffleWorker)

    var requestWorkerCount = Math.ceil(numPartitions / partitionCountPerShuffleWorker.toDouble).toInt

    if (requestWorkerCount < minServerCount) {
      requestWorkerCount = minServerCount
    }

    if (requestWorkerCount > maxServerCount) {
      requestWorkerCount = maxServerCount
    }

    val configuredServerList = ShuffleUtils.getShuffleServersWithoutCheck(
      getOrCreateServiceManager,
      requestWorkerCount,
      networkTimeoutMillis,
      dataCenter,
      cluster,
      sparkConf.getAppId,
      dagId,
      jobPriority,
      taskId,
      appName
    )

    if (configuredServerList.getServerDetailList.isEmpty) {
      throw new Ors2NoShuffleWorkersException("There is no reachable ors2 server")
    }

    val clusterConf = if (StringUtils.isEmpty(configuredServerList.getConf)) {
      Ors2ClusterConf(dfsDirPrefix, dataCenter, cluster, "")
    } else {
      Ors2ClusterConf(configuredServerList.getRootDir, configuredServerList.getDataCenter, configuredServerList.getCluster, configuredServerList.getConf)
    }
    log.info(s"use shuffle cluster: $clusterConf")

    (clusterConf, configuredServerList.getServerDetailArray)
  }
}
