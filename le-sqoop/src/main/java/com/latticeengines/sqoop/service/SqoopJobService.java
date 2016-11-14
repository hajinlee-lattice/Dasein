package com.latticeengines.sqoop.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;

public interface SqoopJobService {
    ApplicationId exportData(SqoopExporter exporter);
    ApplicationId importData(SqoopImporter exporter);
}
