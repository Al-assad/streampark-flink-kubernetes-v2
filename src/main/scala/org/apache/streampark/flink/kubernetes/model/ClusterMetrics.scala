package org.apache.streampark.flink.kubernetes.model

/**
 * see: org.apache.streampark.flink.kubernetes.model.FlinkMetricCV
 */
case class ClusterMetrics(
    totalJmMemory: Integer = 0,
    totalTmMemory: Integer = 0,
    totalTm: Integer = 0,
    totalSlot: Integer = 0,
    availableSlot: Integer = 0,
    runningJob: Integer = 0,
    finishedJob: Integer = 0,
    cancelledJob: Integer = 0,
    failedJob: Integer = 0)
