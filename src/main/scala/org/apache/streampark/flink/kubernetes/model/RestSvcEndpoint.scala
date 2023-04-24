package org.apache.streampark.flink.kubernetes.model

import org.apache.streampark.flink.kubernetes.observer.{reachFlinkRestType, AccessFlinkRestType}

/**
 * Flink rest service endpoint info on kubernetes.
 */
case class RestSvcEndpoint(namespace: String, name: String, port: Int, clusterIP: String) {

  lazy val dnsRest: String = s"http://$name.$namespace:$port"

  lazy val ipRest: String = s"http://$clusterIP:$port"

  lazy val chooseRest: String = reachFlinkRestType match
    case AccessFlinkRestType.DNS => dnsRest
    case AccessFlinkRestType.IP  => ipRest

}
