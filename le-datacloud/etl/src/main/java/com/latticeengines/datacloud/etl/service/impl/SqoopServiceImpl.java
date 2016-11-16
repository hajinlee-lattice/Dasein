package com.latticeengines.datacloud.etl.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.service.SqoopService;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;

@Component("sqlService")
public class SqoopServiceImpl implements SqoopService {

    @Autowired
    private SqoopJobService sqoopJobService;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public ApplicationId importTable(SqoopImporter importer) {
        importer.setYarnConfiguration(yarnConfiguration);
        importer.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopJobService.importData(importer);
    }

    @Override
    public ApplicationId exportTable(SqoopExporter exporter) {
        exporter.setYarnConfiguration(yarnConfiguration);
        exporter.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopJobService.exportData(exporter);
    }

}
