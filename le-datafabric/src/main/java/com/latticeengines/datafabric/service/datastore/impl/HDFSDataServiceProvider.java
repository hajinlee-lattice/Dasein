package com.latticeengines.datafabric.service.datastore.impl;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;

import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;

@Component("hdfsDataServiceProvider")
public class HDFSDataServiceProvider implements FabricDataServiceProvider {

    private static final Logger log = LoggerFactory.getLogger(FabricDataServiceImpl.class);

    private Configuration config;

    private String repositoryDir;

    private String baseDir;

    public HDFSDataServiceProvider() {
    }

    public HDFSDataServiceProvider(Configuration config, String baseDir, String repositoryDir) {
        this.config = config;
        this.baseDir = baseDir;
        this.repositoryDir = repositoryDir;
    }

    @Override
    public String getName() {
        return FabricStoreEnum.HDFS.name();
    }

    @Override
    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {
        log.info("Initialize HDFS data store " + " repo " + repository + " record " + recordType);
        return new HDFSDataStoreImpl(config, baseDir, repositoryDir, repository, recordType, schema);
    }

}
