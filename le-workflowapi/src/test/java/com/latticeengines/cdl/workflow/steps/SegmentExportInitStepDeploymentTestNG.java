package com.latticeengines.cdl.workflow.steps;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.export.SegmentExportProcessor;
import com.latticeengines.cdl.workflow.steps.export.SegmentExportProcessorFactory;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.AtlasExportType;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport.Status;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.SegmentExportStepConfiguration;
import com.latticeengines.proxy.exposed.pls.PlsInternalProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({GlobalAuthCleanupTestListener.class})
@TestExecutionListeners({DirtiesContextTestExecutionListener.class})
@ContextConfiguration(locations = { //
        "classpath:playmakercore-context.xml", "classpath:test-playlaunch-properties-context.xml",
        "classpath:yarn-context.xml", "classpath:proxy-context.xml", "classpath:test-workflowapi-context.xml",
        "classpath:test-testframework-cleanup-context.xml"})
public class SegmentExportInitStepDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(SegmentExportInitStepDeploymentTestNG.class);

    private SegmentExportInitStep segmentExportInitStep;

    @Inject
    private TestMetadataSegmentProxy testMetadataSegmentProxy;

    @Inject
    private PlsInternalProxy plsInternalProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SegmentExportProcessorFactory segmentExportProcessorFactory;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    private Tenant tenant;

    private CustomerSpace customerSpace;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();
        testPlayCreationHelper.setupTestSegment();
        testPlayCreationHelper.setupTestRulesBasedModel();
        tenant = testPlayCreationHelper.getTenant();

        customerSpace = CustomerSpace.parse(tenant.getId());
        testPlayCreationHelper.getDeploymentTestBed().attachProtectedProxy(testMetadataSegmentProxy);
    }

    @Test(groups = "deployment")
    public void testAccountExport() throws IOException {
        MetadataSegmentExport segmentExport = createExportJob(customerSpace, AtlasExportType.ACCOUNT);
        segmentExportInitStep.execute(yarnConfiguration);
        confirmJobSuccessful(segmentExport,
                BusinessEntity.Account.name() + SegmentExportProcessor.SEPARATOR + InterfaceName.AccountId.name());
    }

    @Test(groups = "deployment", dependsOnMethods = {"testAccountExport"})
    public void testContactExport() throws IOException {
        MetadataSegmentExport segmentExport = createExportJob(customerSpace, AtlasExportType.CONTACT);
        segmentExportInitStep.execute(yarnConfiguration);
        confirmJobSuccessful(segmentExport,
                BusinessEntity.Contact.name() + SegmentExportProcessor.SEPARATOR + InterfaceName.AccountId.name(),
                BusinessEntity.Contact.name() + SegmentExportProcessor.SEPARATOR + InterfaceName.ContactId.name());
    }

    @Test(groups = "deployment", dependsOnMethods = {"testContactExport"})
    public void testAccountAndContactExport() throws IOException {
        MetadataSegmentExport segmentExport = createExportJob(customerSpace, AtlasExportType.ACCOUNT_AND_CONTACT);
        segmentExportInitStep.execute(yarnConfiguration);
        confirmJobSuccessful(segmentExport,
                BusinessEntity.Contact.name() + SegmentExportProcessor.SEPARATOR + InterfaceName.AccountId.name(),
                BusinessEntity.Contact.name() + SegmentExportProcessor.SEPARATOR + InterfaceName.ContactId.name(),
                BusinessEntity.Account.name() + SegmentExportProcessor.SEPARATOR + InterfaceName.AccountId.name());
    }

    private MetadataSegmentExport createExportJob(CustomerSpace customerSpace, AtlasExportType type) {
        String segmentName = UUID.randomUUID().toString();
        MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
        metadataSegmentExport.setType(type);
        metadataSegmentExport
                .setAccountFrontEndRestriction(testPlayCreationHelper.getSegment().getAccountFrontEndRestriction());
        metadataSegmentExport
                .setContactFrontEndRestriction(testPlayCreationHelper.getSegment().getContactFrontEndRestriction());
        metadataSegmentExport.setStatus(Status.RUNNING);
        metadataSegmentExport.setExportPrefix(segmentName);
        metadataSegmentExport.setCreatedBy("CREATED_BY");
        metadataSegmentExport.setCleanupBy(new Date(System.currentTimeMillis() + 7 * 24 * 60 * 60 * 1000));

        metadataSegmentExport = testMetadataSegmentProxy.createSegmentExport(metadataSegmentExport, false);

        Assert.assertNotNull(metadataSegmentExport.getExportId());
        Assert.assertNotNull(metadataSegmentExport);
        Assert.assertNotNull(metadataSegmentExport.getExportId());

        SegmentExportStepConfiguration conf = new SegmentExportStepConfiguration();
        conf.setCustomerSpace(customerSpace);
        conf.setMetadataSegmentExport(metadataSegmentExport);
        conf.setMetadataSegmentExportId(metadataSegmentExport.getExportId());
        conf.setName(metadataSegmentExport.getFileName() == null ? UUID.randomUUID().toString()
                : metadataSegmentExport.getFileName());

        segmentExportInitStep = new SegmentExportInitStep();
        segmentExportInitStep.setTenantEntityMgr(tenantEntityMgr);
        segmentExportInitStep.setConfiguration(conf);
        segmentExportInitStep.setSegmentExportProcessorFactory(segmentExportProcessorFactory);
        segmentExportInitStep.setPlsInternalProxy(plsInternalProxy);
        return metadataSegmentExport;
    }

    private void confirmJobSuccessful(MetadataSegmentExport segmentExport, String... fewColumns) throws IOException {
        segmentExport = testMetadataSegmentProxy.getSegmentExport(segmentExport.getExportId());
        Assert.assertNotNull(segmentExport);
        Assert.assertNotNull(segmentExport.getStatus());
        Assert.assertEquals(segmentExport.getStatus(), Status.COMPLETED);
        String avroFilePath = segmentExportProcessorFactory.getProcessor( //
                segmentExport.getType()).getAvroFilePath();
        Assert.assertNotNull(avroFilePath);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroFilePath), avroFilePath);
        log.info(String.format("avroFilePath = %s, HdfsUtils.getFileSize = %s", avroFilePath,
                HdfsUtils.getFileSize(yarnConfiguration, avroFilePath)));
        String tempLocalFileName = "ExportedAvroFile_" + UUID.randomUUID().toString();
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, avroFilePath, tempLocalFileName);

        File localFile = new File(tempLocalFileName);
        GenericDatumReader<Object> datumReader = new GenericDatumReader<Object>();
        DataFileReader<?> avroReader = new DataFileReader<>(localFile, datumReader);
        Iterator<?> iterator = avroReader.iterator();
        int rowCount = 0;
        String ratingId = testPlayCreationHelper.getRulesBasedRatingEngine().getId();
        String ratingAttribute = "Rating" + SegmentExportProcessor.SEPARATOR + ratingId;

        boolean shouldCheckRating = //
                segmentExport.getType() == AtlasExportType.ACCOUNT_AND_CONTACT
                        || segmentExport.getType() == AtlasExportType.ACCOUNT;
        int notNullRatingCount = 0;
        while (iterator.hasNext()) {
            Object row = iterator.next();
            Assert.assertNotNull(row);
            Assert.assertEquals(row.getClass(), GenericData.Record.class);
            GenericData.Record record = (GenericData.Record) row;
            for (String col : fewColumns) {
                Object val = record.get(col);
                Assert.assertNotNull(val);
                Assert.assertTrue(StringUtils.isNotBlank(val.toString()));
            }

            if (shouldCheckRating) {
                Object ratingVal = record.get(ratingAttribute);
                if (ratingVal != null) {
                    String ratingValString = ratingVal.toString();
                    RatingBucketName bucket = RatingBucketName.valueOf(ratingValString);
                    Assert.assertNotNull(bucket);
                    notNullRatingCount++;
                }
            }
            rowCount++;
        }
        avroReader.close();
        localFile.delete();

        Assert.assertNotEquals(rowCount, 0);
        if (shouldCheckRating) {
            Assert.assertNotEquals(notNullRatingCount, 0);
        }
    }

    @AfterClass(groups = {"deployment"})
    public void teardown() {
        testPlayCreationHelper.cleanupArtifacts(true);
    }
}
