package com.latticeengines.apps.cdl.controller;

import com.latticeengines.apps.cdl.service.impl.CheckpointService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.squareup.moshi.Json;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

public class OrphanRecordExportDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(OrphanRecordExportDeploymentTestNG.class);
    public static final String CHECK_POINT = "orphan";
    private final static String CREATED_BY = "lattice@lattice-engines.com";
    private static final String DEFAULT_EXPORT_FILE_PREFIX = "orphanRecord";
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd_HH-mm-ss";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    @Inject
    private CDLProxy cdlProxy;

    private String segmentName;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;


    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private CheckpointService checkpointService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        super.setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
        log.info("orphan tenant is:" + MultiTenantContext.getTenant().getId());
    }

    @AfterClass(groups = "deployment")
    protected void cleanup() throws Exception {
        checkpointService.cleanup();
    }

    @Test(groups = "deployment")
    public void testOrphanRecordExport() throws Exception {
        checkpointService.resumeCheckpoint(CHECK_POINT, 19);

        log.info("orphan tenant2 is:" + MultiTenantContext.getTenant().getId());
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        MetadataSegmentExport segmentExport = createExportJob(MetadataSegmentExportType.CONTACT);
        log.info(JsonUtils.serialize(segmentExport));
        segmentExport = internalResourceRestApiProxy.createOrphanRecordThruMgr(segmentExport,CustomerSpace.parse(mainTestTenant.getId()));

        ApplicationId appid = cdlProxy.OrphanRecordExport(customerSpace,segmentExport);
        log.info("appid:" + appid.toString());
        log.info("segmentexport:" + JsonUtils.serialize(segmentExport));

        JobStatus status = waitForWorkflowStatus(appid.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        verifyResults();

    }

    private void verifyResults(){
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String dir = String.format("/Pods/Default/Contracts/%s/Tenants/%s/Spaces/Production/Data/Files/Exports",
                tenantId,tenantId);
        try {
            List<String> csvfiles = HdfsUtils.getFilesForDir(yarnConfiguration,dir,".*.csv$");

            Assert.assertNotNull(csvfiles);
            Assert.assertEquals(csvfiles.size(), 5);
            int totalRecordNum = 0;
            for (String filePath:csvfiles){
                CSVParser parser = new CSVParser(
                        new InputStreamReader(HdfsUtils.getInputStream(yarnConfiguration, filePath)), LECSVFormat.format);
                List<CSVRecord> records = parser.getRecords();
                totalRecordNum += records.size();
            }
            Assert.assertEquals(totalRecordNum,17214);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private MetadataSegmentExport createExportJob(MetadataSegmentExportType type) {
        segmentName = UUID.randomUUID().toString();
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setType(type);
        metadataSegmentExport
                .setAccountFrontEndRestriction(new FrontEndRestriction());
        metadataSegmentExport
                .setContactFrontEndRestriction(new FrontEndRestriction());
        metadataSegmentExport.setStatus(MetadataSegmentExport.Status.RUNNING);
        metadataSegmentExport.setExportPrefix(segmentName);
        metadataSegmentExport.setCreatedBy(CREATED_BY);
        metadataSegmentExport.setCleanupBy(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));

        String displayName = "";
        if (metadataSegmentExport.getSegment() != null) {
            displayName = metadataSegmentExport.getSegment().getDisplayName();
        }
        String exportedFileName = constructFileName(metadataSegmentExport.getExportPrefix(),
                displayName, metadataSegmentExport.getType());
        String tableName = NamingUtils.timestamp("orphan_record_export_") ;
        metadataSegmentExport.setFileName(exportedFileName);
        metadataSegmentExport.setTableName(tableName);
        create(metadataSegmentExport);

        return metadataSegmentExport;
    }

    private void create(MetadataSegmentExport segmentExport){
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String path = PathBuilder.buildDataFileUniqueExportPath(CamilleEnvironment.getPodId(), customerSpace)
                .toString();
        segmentExport.setPath(path);
        segmentExport.setTenant(mainTestTenant);
        segmentExport.setTenantId(mainTestTenant.getPid());
    }

    public static String constructFileName(String exportPrefix, String segmentDisplayName,
            MetadataSegmentExportType type) {
        String exportedFileName = null;
        if (StringUtils.isNotBlank(exportPrefix)) {
            exportedFileName = exportPrefix;
        } else if (StringUtils.isNotEmpty(segmentDisplayName)) {
            exportedFileName = segmentDisplayName;
        }

        if (StringUtils.isBlank(exportedFileName)) {
            exportedFileName = DEFAULT_EXPORT_FILE_PREFIX;
        }
        exportedFileName = exportedFileName.trim().replaceAll("[^a-zA-Z0-9]", "");
        exportedFileName += "-" + type + "-" + dateFormat.format(new Date()) + "_UTC.csv";
        return exportedFileName;
    }
}
