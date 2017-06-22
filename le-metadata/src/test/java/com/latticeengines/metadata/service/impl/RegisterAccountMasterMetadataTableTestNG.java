package com.latticeengines.metadata.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.metadata.service.DataCollectionService;
import com.latticeengines.metadata.service.MetadataService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class RegisterAccountMasterMetadataTableTestNG extends MetadataFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(RegisterAccountMasterMetadataTableTestNG.class);

    @Autowired
    private MetadataService mdService;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private DataCollectionService dataCollectionService;

    private String tableName = "redshift_bucketedaccountmaster";
    private String customerSpace;

    @Test(groups = "registertable")
    public void registerMetadataTable() throws Exception {
        createServiceTenant();
        registerBucketedAM();
        bootstrapDataCollection();
    }

    private void createServiceTenant() {
        Tenant tenant = tenantEntityMgr.findByTenantId(DataCloudConstants.SERVICE_CUSTOMERSPACE);
        if (tenant == null) {
            tenant = new Tenant();
            tenant.setName(DataCloudConstants.SERVICE_TENANT);
            tenant.setId(DataCloudConstants.SERVICE_CUSTOMERSPACE);
            tenantEntityMgr.create(tenant);
            tenant = tenantEntityMgr.findByTenantId(DataCloudConstants.SERVICE_CUSTOMERSPACE);
        }
        customerSpace = CustomerSpace.parse(tenant.getId()).toString();
        MultiTenantContext.setTenant(tenant);
    }

    private void registerBucketedAM() throws IOException {
        File file = unzipFile("AccountMasterBucketed.avsc");
        Configuration config = new Configuration();
        config.set(FileSystem.FS_DEFAULT_NAME_KEY, FileSystem.DEFAULT_FS);
        Table bucketedTable = MetadataConverter.getBucketedTableFromSchemaPath(config, file.getPath(), null, null);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage(tableName);
        bucketedTable.setName(tableName);
        bucketedTable.setStorageMechanism(storage);
        bucketedTable.setInterpretation(SchemaInterpretation.AccountMaster.toString());
        log.info("Registering AccountMaster Bucketed Metadata Table");
        mdService.updateTable(CustomerSpace.parse(DataCloudConstants.SERVICE_TENANT), bucketedTable);
        FileUtils.deleteQuietly(file);
    }

    private void bootstrapDataCollection() throws IOException {
        DataCollection collection = dataCollectionService.getDataCollection(customerSpace, "");
        File file = unzipFile("amstats.json");
        ObjectMapper om = new ObjectMapper();
        Statistics statistics = om.readValue(file, Statistics.class);
        FileUtils.deleteQuietly(file);
        StatisticsContainer container = new StatisticsContainer();
        container.setStatistics(statistics);
        dataCollectionService.upsertTable(customerSpace, collection.getName(), tableName, TableRoleInCollection.AccountMaster);
        dataCollectionService.addStats(customerSpace, collection.getName(), container, null);
    }

    private File unzipFile(String amArtifact) throws IOException {
        File currPath = new File(System.getProperty("user.dir"));
        File zipFile = new File(currPath.getParentFile().getAbsolutePath() + "/le-dev/testartifacts/AccountMaster/"
                + amArtifact + ".gz");
        File file = new File(
                currPath.getParentFile().getAbsolutePath() + "/le-dev/testartifacts/AccountMaster/" + amArtifact);
        InputStream is = new GZIPInputStream(new FileInputStream(zipFile));
        FileUtils.copyInputStreamToFile(is, file);
        return file;
    }

}
