package com.latticeengines.datacloud.etl.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.service.SqoopService;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("sqlService")
public class SqoopServiceImpl implements SqoopService {

    @Autowired
    private SqoopProxy sqoopProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public ApplicationId importTable(SqoopImporter importer) {
        importer.setYarnConfiguration(yarnConfiguration);
        importer.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return ConverterUtils.toApplicationId(sqoopProxy.importData(importer).getApplicationIds().get(0));
    }

    @Override
    public ApplicationId exportTable(SqoopExporter exporter) {
        exporter.setYarnConfiguration(yarnConfiguration);
        exporter.setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission());
        return ConverterUtils.toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
    }

}
