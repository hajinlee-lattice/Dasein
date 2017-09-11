package com.latticeengines.datacloud.yarn.service.impl;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Resource;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.ColumnSelectionService;
import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.datacloud.yarn.testframework.DataCloudYarnFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Column;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.datacloud.match.UnionSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class DataCloudYarnServiceImplTestNG extends DataCloudYarnFunctionalTestNGBase {

    private static final String avroDir = "/tmp/PropDataYarnServiceTestNG";
    private static final String podId = "PropDataYarnServiceImplTestNG";

    @Autowired
    private DataCloudYarnService dataCloudYarnService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Resource(name = "accountMasterColumnSelectionService")
    private ColumnSelectionService columnSelectionService;

    @Value("${datacloud.match.latest.data.cloud.major.version}")
    private String latestMajorVersion;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        switchHdfsPod(podId);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPathBuilder.podDir().toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        }
    }

    @Test(groups = "functional")
    public void testMatchBlockInYarnContainer() throws Exception {
        String fileName = "BulkMatchInput.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;

        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);
        jobConfiguration.getMatchInput().setPrepareForDedupe(true);
        jobConfiguration.getMatchInput().setRequestSource(MatchRequestSource.MODELING);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        verifyDedupeHelpers(jobConfiguration);
    }

    @Test(groups = "functional")
    public void testRTSNoMatch() throws Exception {
        String fileName = "BulkMatchInput_NoMatch.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;
        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);
        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "functional")
    public void testEnrichment() throws Exception {
        String fileName = "BulkMatchInput.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;

        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);

        UnionSelection unionSelection = new UnionSelection();
        unionSelection.setPredefinedSelections(ImmutableMap.of(Predefined.ID, "1.0"));
        ColumnSelection allColumns = columnSelectionService.parsePredefinedColumnSelection(Predefined.Enrichment,
                versionEntityMgr.currentApprovedVersionAsString());
        ColumnSelection customSelection = new ColumnSelection();
        customSelection.setColumns(allColumns.getColumns().subList(0, 10));
        unionSelection.setCustomSelection(customSelection);
        jobConfiguration.getMatchInput().setUnionSelection(unionSelection);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String avroGlob = getBlockOutputDir(jobConfiguration) + "/*.avro";
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, avroGlob);
        while (records.hasNext()) {
            GenericRecord record = records.next();
            for (int i = 0; i < record.getSchema().getFields().size(); i++) {
                Object obj = record.get(i);
                if (obj instanceof Utf8 || obj instanceof String) {
                    String str = obj.toString();
                    Assert.assertFalse("null".equalsIgnoreCase(str),
                            record.getSchema().getFields().get(i).name() + " is the literal \"null\"!");
                }
            }
        }
    }

    @Test(groups = "functional")
    public void testSegment() throws Exception {
        String fileName = "BulkMatchInput_WithIds.avro";
        cleanupAvroDir(avroDir);
        updateAvroFile(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;

        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);

        UnionSelection unionSelection = new UnionSelection();
        unionSelection.setPredefinedSelections(ImmutableMap.of(Predefined.Segment, "2.0"));
        ColumnSelection cs = new ColumnSelection();
        List<Column> cols = Arrays.asList(new Column(DataCloudConstants.ATTR_LDC_DOMAIN),
                new Column(DataCloudConstants.ATTR_LDC_NAME));
        cs.setColumns(cols);
        unionSelection.setCustomSelection(cs);
        jobConfiguration.getMatchInput().setPredefinedSelection(null);
        jobConfiguration.getMatchInput().setUnionSelection(unionSelection);
        jobConfiguration.getMatchInput().setFetchOnly(true);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String avroGlob = getBlockOutputDir(jobConfiguration) + "/*.avro";
        Iterator<GenericRecord> records = AvroUtils.iterator(yarnConfiguration, avroGlob);
        Long count = 0L;
        Long notNullLdcDomain = 0L;
        Long notNullLdcName = 0L;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            count++;
            if (record.get(DataCloudConstants.ATTR_LDC_NAME) != null
                    && StringUtils.isNotBlank(record.get(DataCloudConstants.ATTR_LDC_NAME).toString())) {
                notNullLdcName++;
            }
            if (record.get(DataCloudConstants.ATTR_LDC_DOMAIN) != null
                    && StringUtils.isNotBlank(record.get(DataCloudConstants.ATTR_LDC_DOMAIN).toString())) {
                notNullLdcDomain++;
            }
        }
        Assert.assertTrue(notNullLdcDomain.doubleValue() / count.doubleValue() > 0.9, String.format(
                "Only %.2f %% records has LDC_Domain", 100 * notNullLdcDomain.doubleValue() / count.doubleValue()));
        Assert.assertTrue(notNullLdcName.doubleValue() / count.doubleValue() > 0.9, String
                .format("Only %.2f %% records has LDC_Name", 100 * notNullLdcName.doubleValue() / count.doubleValue()));
    }

    private void verifyDedupeHelpers(DataCloudJobConfiguration jobConfiguration) throws Exception {
        String rootUid = jobConfiguration.getRootOperationUid();
        String blockUid = jobConfiguration.getBlockOperationUid();
        String blockDir = hdfsPathBuilder.constructMatchBlockDir(rootUid, blockUid).toString();
        AvroUtils.iterator(yarnConfiguration, blockDir + "/*.avro").forEachRemaining(record -> {
            Object id = record.get(MatchConstants.INT_LDC_LID);
            Object dedupeId = record.get(MatchConstants.INT_LDC_DEDUPE_ID);
            Object isRemoved = record.get(MatchConstants.INT_LDC_REMOVED);
            System.out.println(record.get("ID") + " : " + id + " - " + dedupeId + " - " + isRemoved);
            Assert.assertTrue(id != null || dedupeId != null);
            Assert.assertTrue(isRemoved instanceof Integer);
            Assert.assertEquals(isRemoved, 0);
        });
    }

    private String getBlockOutputDir(DataCloudJobConfiguration jobConfiguration) {
        String rootUid = jobConfiguration.getRootOperationUid();
        String blockUid = jobConfiguration.getBlockOperationUid();
        return hdfsPathBuilder.constructMatchBlockDir(rootUid, blockUid).toString();
    }

    private DataCloudJobConfiguration jobConfiguration(String avroPath) {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(schema);

        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setPredefinedSelection(Predefined.RTS);
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setKeyMap(keyMap);
        matchInput.setSplitsPerBlock(8);
        DataCloudJobConfiguration jobConfiguration = new DataCloudJobConfiguration();
        jobConfiguration.setHdfsPodId(podId);
        jobConfiguration.setName("DataCloudMatchBlock");
        jobConfiguration.setCustomerSpace(CustomerSpace.parse("LDCTest"));
        jobConfiguration.setAvroPath(avroPath);
        jobConfiguration.setBlockSize(AvroUtils.count(yarnConfiguration, avroPath).intValue());
        jobConfiguration.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setBlockOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setThreadPoolSize(4);
        jobConfiguration.setGroupSize(10);
        jobConfiguration.setMatchInput(matchInput);

        return jobConfiguration;
    }

}
