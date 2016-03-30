package com.latticeengines.propdata.core.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.propdata.core.service.SqlService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("sqlService")
public class SqlServiceImpl implements SqlService {

    @Autowired
    private SqoopSyncJobService sqoopService;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public ApplicationId importTable(SqoopImporter importer) {
        importer.setYarnConfiguration(yarnConfiguration);
        importer.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        System.out.println(JsonUtils.serialize(importer));
        return sqoopService.importData(importer);
    }

    @Override
    public ApplicationId exportTable(SqoopExporter exporter) {
        exporter.setYarnConfiguration(yarnConfiguration);
        exporter.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return sqoopService.exportData(exporter);
    }

}
