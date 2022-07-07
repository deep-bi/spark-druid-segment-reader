package org.apache.spark.util

import org.apache.hadoop.conf.Configuration

class SerializableHadoopConfiguration(private val serializableConfig: SerializableConfiguration) extends Serializable {

  def config: Configuration = serializableConfig.value
}

object SerializableHadoopConfiguration {

  def apply(hadoopConf: Configuration): SerializableHadoopConfiguration = {
    new SerializableHadoopConfiguration(new SerializableConfiguration(hadoopConf))
  }
}