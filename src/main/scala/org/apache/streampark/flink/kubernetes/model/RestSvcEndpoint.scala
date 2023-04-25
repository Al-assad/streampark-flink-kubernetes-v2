package org.apache.streampark.flink.kubernetes.model

import org.apache.streampark.flink.kubernetes.observer.{reachFlinkRestType, AccessFlinkRestType}

/**
 * Flink rest service endpoint info on kubernetes.
 */
case class RestSvcEndpoint(namespace: String, name: String, port: Int, clusterIP: String) {

  lazy val dns: String = s"$name.$namespace"

  lazy val dnsRest: String = s"http://$dns:$port"

  lazy val ipRest: String = s"http://$clusterIP:$port"

  def chooseRest: String = reachFlinkRestType match
    case AccessFlinkRestType.DNS => dnsRest
    case AccessFlinkRestType.IP  => ipRest

  def chooseHost: String = reachFlinkRestType match
    case AccessFlinkRestType.DNS => dns
    case AccessFlinkRestType.IP  => clusterIP

}
