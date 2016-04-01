package com.latticeengines.propdata.core.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;

public interface SqoopService {
    ApplicationId importTable(SqoopImporter importer);
    ApplicationId exportTable(SqoopExporter exporter);
}
