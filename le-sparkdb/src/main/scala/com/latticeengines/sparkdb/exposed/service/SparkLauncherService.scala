package com.latticeengines.sparkdb.exposed.service

import org.apache.hadoop.yarn.api.records.ApplicationId

trait SparkLauncherService {
  def runApp(appName: String, queue: String): ApplicationId
}