package com.latticeengines.datafabric.service.datastore.impl;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datafabric.service.datastore.FabricDataServiceProvider;
import com.latticeengines.datafabric.service.datastore.FabricDataStore;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;

//@Component("s3DataServiceProvider")
public class S3DataServiceProvider implements FabricDataServiceProvider {

    private static final Logger log = LoggerFactory.getLogger(S3DataServiceProvider.class);

    private Configuration config;

    private String repositoryDir;

    private String baseDir;
    private String localDir;

    public S3DataServiceProvider(Configuration config, String baseDir, String localDir, String repositoryDir) {
        this.config = config;
        this.baseDir = baseDir;
        this.localDir = localDir;
        this.repositoryDir = repositoryDir;
    }

    @Override
    public String getName() {
        return FabricStoreEnum.S3.name();
    }

    @Override
    public FabricDataStore constructDataStore(String repository, String recordType, Schema schema) {
        if (log.isDebugEnabled()) {
            log.debug("Initialize S3 data store " + " repo " + repository + " record " + recordType);
        }
        return new HDFSDataStoreImpl(config, baseDir, localDir, repositoryDir, repository, recordType, schema, false);
    }

}
