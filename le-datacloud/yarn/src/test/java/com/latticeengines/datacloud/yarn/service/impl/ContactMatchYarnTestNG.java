package com.latticeengines.datacloud.yarn.service.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.datacloud.yarn.testframework.DataCloudYarnFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

public class ContactMatchYarnTestNG extends DataCloudYarnFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ContactMatchYarnTestNG.class);

    private static final String avroDir = "/tmp/PrimeMatchYarnTestNG";
    private static final String podId = "PrimeMatchYarnTestNG";

    @Inject
    private DataCloudYarnService dataCloudYarnService;

    @Value("${datacloud.match.default.decision.graph.tps}")
    private String tpsMatchDG;

    @BeforeClass(groups = {"functional", "manual"})
    public void setup() throws Exception {
        switchHdfsPod(podId);
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPathBuilder.podDir().toString())) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPathBuilder.podDir().toString());
        }
    }

    @Test(groups = "functional")
    public void testTpsMatch() {
        String fileName = "TpsMatchInput.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);
        String avroPath = avroDir + "/" + fileName;

        DataCloudJobConfiguration jobConfiguration = jobConfiguration(avroPath);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String avroGlob = getBlockOutputDir(jobConfiguration) + "/*.avro";
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, avroGlob);
        log.info("Fields: {}", StringUtils.join(schema.getFields().stream() //
                .map(Schema.Field::name).collect(Collectors.toList()), ","));
        Iterator<GenericRecord> records = AvroUtils.iterateAvroFiles(yarnConfiguration, avroGlob);
        long count = 0L;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            System.out.println(record);
            String recordId = record.get(ContactMasterConstants.TPS_ATTR_RECORD_ID).toString();
            Assert.assertNotNull(recordId);
            //FIXME [M39-LiveRamp]: for true data, add more assertion
            count++;
        }
        Assert.assertTrue(count > 0);
    }

    private String getBlockOutputDir(DataCloudJobConfiguration jobConfiguration) {
        String rootUid = jobConfiguration.getRootOperationUid();
        String blockUid = jobConfiguration.getBlockOperationUid();
        return hdfsPathBuilder.constructMatchBlockDir(rootUid, blockUid).toString();
    }

    private DataCloudJobConfiguration jobConfiguration(String avroPath) {
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));

        MatchInput matchInput = new MatchInput();

        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        matchInput.setOperationalMode(OperationalMode.CONTACT_MATCH);
        matchInput.setTargetEntity(ContactMasterConstants.MATCH_ENTITY_TPS);

        Map<MatchKey, List<String>> keyMap = new HashMap<>();
        keyMap.put(MatchKey.DUNS, Collections.singletonList("SiteDuns"));
        matchInput.setKeyMap(keyMap);
        matchInput.setSkipKeyResolution(true);

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
