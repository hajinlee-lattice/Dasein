package com.latticeengines.sqoop.exposed.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;

public interface SqoopJobService {

    ApplicationId exportData(SqoopExporter exporter, Configuration yarnConfiguration);

    ApplicationId importData(SqoopImporter importer, Configuration yarnConfiguration);

}
