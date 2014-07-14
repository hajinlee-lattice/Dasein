package com.latticeengines.sparkdb.exposed.service

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ApplicationId

trait SparkLauncherService {
  def runApp(conf: Configuration, appName: String, queue: String): ApplicationId
}