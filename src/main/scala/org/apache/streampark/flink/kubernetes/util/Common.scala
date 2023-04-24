package org.apache.streampark.flink.kubernetes.util

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature

import scala.language.implicitConversions
import util.chaining.scalaUtilChainingOps

val jacksonMapper: ObjectMapper = ObjectMapper()

val yamlMapper: ObjectMapper = ObjectMapper(YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER))
  .tap(_.setSerializationInclusion(JsonInclude.Include.NON_NULL))

implicit class PrettyStringExtension(value: AnyRef) {
  def prettyStr: String = value match
    case v: String => v
    case v         => pprint.apply(v, height = 2000).render
}

implicit def liftValueAsSome[A](value: A): Option[A] = Some(value)

def pathLastSegment(path: String): String = path.split("/").last
