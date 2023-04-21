package org.apache.streampark.flink.kubernetes.model

/**
 * Flink kubernetes resource tracking id which is a unique value defined in the StreamPark system.
 * It's id attribute depends on the specific resource.
 *
 *  - [[ApplicationJobKey]]: ref to [[org.apache.streampark.console.core.entity.Application.id]]
 *  - [[SessionJobKey]]: ref to [[org.apache.streampark.console.core.entity.Application.id]]
 *  - [[UnmanagedJobKey]]: ref to [[org.apache.streampark.console.core.entity.Application.id]]
 *  - [[ClusterKey]]: ref to [[org.apache.streampark.console.core.entity.FlinkCluster.id]]
 */
sealed trait TrackKey {
  val id: Long
}

object TrackKey {

  def appJob(id: Long, namespace: String, name: String): ApplicationJobKey                      = ApplicationJobKey(id, namespace, name)
  def sessionJob(id: Long, namespace: String, name: String, clusterName: String): SessionJobKey = SessionJobKey(id, namespace, name, clusterName)
  def cluster(id: Long, namespace: String, name: String): ClusterKey                            = ClusterKey(id, namespace, name)

  def unmanagedSessionJob(id: Long, clusterNs: String, clusterName: String, jid: String): UnmanagedSessionJobKey =
    UnmanagedSessionJobKey(id, clusterNs, clusterName, jid)

  case class ApplicationJobKey(id: Long, namespace: String, name: String)                  extends TrackKey
  case class SessionJobKey(id: Long, namespace: String, name: String, clusterName: String) extends TrackKey
  case class ClusterKey(id: Long, namespace: String, name: String)                         extends TrackKey

  // Compatible with previous versions of tasks submitted directly to flink-k8s-session
  case class UnmanagedSessionJobKey(id: Long, clusterNs: String, clusterId: String, jid: String) extends TrackKey
}
