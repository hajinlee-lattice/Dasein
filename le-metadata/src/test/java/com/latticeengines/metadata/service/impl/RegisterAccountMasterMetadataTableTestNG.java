package com.latticeengines.metadata.service.impl;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.MetadataService;

public class RegisterAccountMasterMetadataTableTestNG extends MetadataFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(RegisterAccountMasterMetadataTableTestNG.class);

    @Autowired
    private MetadataService mdService;

    @Test(groups = "registertable")
    public void registerMetadataTable() {
        File file = new File(
                System.getenv("WSHOME") + "/le-dev/testartifacts/AccountMaster/AccountMasterBucketed.avsc");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table bucketedTable = MetadataConverter.getBucketedTableFromSchemaPath(config, file.getPath(), null, null);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage("redshift_bucketedaccountmaster");
        bucketedTable.setStorageMechanism(storage);

        log.info("Registering AccountMaster Bucketed Metadata Table");
        mdService.createTable(CustomerSpace.parse(DataCloudConstants.SERVICE_TENANT), bucketedTable);
    }
}
