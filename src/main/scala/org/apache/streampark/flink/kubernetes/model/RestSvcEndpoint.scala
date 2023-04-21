package org.apache.streampark.flink.kubernetes.model

/**
 * Flink rest service endpoint info on kubernetes.
 */
case class RestSvcEndpoint(namespace: String, name: String, port: Int, clusterIP: String) {

  lazy val dnsRest = s"http://$name.$namespace:$port"

  lazy val ipRest = s"http://$clusterIP:$port"

}
