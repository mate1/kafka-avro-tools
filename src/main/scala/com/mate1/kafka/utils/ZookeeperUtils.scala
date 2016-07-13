package com.mate1.kafka.utils

import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZkUtils}

import scala.collection.{Map, immutable}

/**
 * Created by Marc-Andre Lamothe on 3/31/15.
 */
object ZookeeperUtils {

  def getConsumerLag(zkUtils: ZkUtils, group: String, topic: String): Option[Long] = {
    try {
      zkUtils.getPartitionsForTopics(Seq(topic)).get(topic) match {
        case Some(pids) =>
          (recoverOffsetFromZK(zkUtils, group, topic, pids), recoverOffsetFromKafka(zkUtils, topic, pids, OffsetRequest.LatestTime)) match {
            case (Some(consOffset: Long), Some(maxOffset: Long)) =>
              Some(maxOffset - consOffset)
            case _ =>
              None
          }
        case _ =>
          None
      }
    } catch {
      case e: Throwable =>
        None
    }
  }

  def getConsumerLagJava(zkUtils: ZkUtils, group: String, topic: String): java.lang.Long = getConsumerLag(zkUtils, group, topic).map(java.lang.Long.valueOf).orNull

  def getConsumerOffset(zkUtils: ZkUtils, group: String, topic: String): Option[Long] = {
    try {
      zkUtils.getPartitionsForTopics(Seq(topic)).get(topic) match {
        case Some(pids) =>
          recoverOffsetFromZK(zkUtils, group, topic, pids)
        case _ =>
          None
      }
    } catch {
      case e: Throwable =>
        None
    }
  }

  def getMaxOffset(zkUtils: ZkUtils, topic: String): Option[Long] = {
    try {
      zkUtils.getPartitionsForTopics(Seq(topic)).get(topic) match {
        case Some(pids) =>
          recoverOffsetFromKafka(zkUtils, topic, pids, OffsetRequest.LatestTime)
        case _ =>
          None
      }
    } catch {
      case e: Throwable =>
        None
    }
  }

  def getMinOffset(zkUtils: ZkUtils, topic: String): Option[Long] = {
    try {
      zkUtils.getPartitionsForTopics(Seq(topic)).get(topic) match {
        case Some(pids) =>
          recoverOffsetFromKafka(zkUtils, topic, pids, OffsetRequest.EarliestTime)
        case _ =>
          None
      }
    } catch {
      case e: Throwable =>
        None
    }
  }

  private def recoverOffsetFromKafka(zkUtils: ZkUtils, topic: String, pids: Seq[Int], offset: Long): Option[Long] = {
    if (pids.nonEmpty) {
      var result: Option[Long] = Some(0)
      for (pid <- pids if result.isDefined) {
        zkUtils.getLeaderForPartition(topic, pid) match {
          case Some(leaderId: Int) =>
            zkUtils.readDataMaybeNull(ZkUtils.BrokerIdsPath + "/" + leaderId)._1 match {
              case Some(brokerInfo: String) =>
                Json.parseFull(brokerInfo) match {
                  case Some(json) =>
                    val brokerInfo = json.asInstanceOf[Map[String, Any]]
                    val simpleConsumer = new SimpleConsumer(brokerInfo.get("host").get.asInstanceOf[String], brokerInfo.get("port").get.asInstanceOf[Int], 10000, 100000, "ZookeeperUtilsOffsetRequester")
                    try {
                      val topicAndPartition = TopicAndPartition(topic, pid)
                      val response = simpleConsumer.getOffsetsBefore(OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(offset, 1))))
                      if (!response.hasError)
                        result = result.map(value => value + response.partitionErrorAndOffsets(topicAndPartition).offsets.head)
                    }
                    catch {
                      case e: Throwable =>
                        e.printStackTrace()
                        result = None
                    }
                    finally {
                      simpleConsumer.close()
                    }
                  case _ =>
                    result = None
                }
              case _ =>
                result = None
            }
          case _ =>
            result = None
        }
      }
      result
    }
    else
      None
  }

  private def recoverOffsetFromZK(zkUtils: ZkUtils, group: String, topic: String, pids: Seq[Int]): Option[Long] = {
    if (pids.nonEmpty) {
      var groupOffset = 0l
      for (pid <- pids if groupOffset >= 0)
        groupOffset += zkUtils.readData("/consumers/%s/offsets/%s/%s".format(group, topic, pid))._1.toLong

      Some(groupOffset)
    }
    else
      None
  }

}
