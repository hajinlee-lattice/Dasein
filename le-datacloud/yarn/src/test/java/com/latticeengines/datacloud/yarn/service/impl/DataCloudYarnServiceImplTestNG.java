package com.latticeengines.datacloud.yarn.service.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.yarn.exposed.service.DataCloudYarnService;
import com.latticeengines.datacloud.yarn.testframework.DataCloudYarnFunctionalTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudJobConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

@Component
public class DataCloudYarnServiceImplTestNG extends DataCloudYarnFunctionalTestNGBase {

    private static final String avroDir = "/tmp/PropDataYarnServiceTestNG";
    private static final String fileName = "BulkMatchInput.avro";
    private static final String podId = "PropDataYarnServiceImplTestNG";

    @Autowired
    private DataCloudYarnService dataCloudYarnService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

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
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);

        String avroPath = avroDir + "/" + fileName;
        String latestDataCloudVersion = versionEntityMgr.latestApprovedForMajorVersion(latestMajorVersion).getVersion();

        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(schema);

        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant("DCTest"));
        matchInput.setPredefinedSelection(Predefined.RTS);
        matchInput.setDataCloudVersion(latestDataCloudVersion);
        matchInput.setKeyMap(keyMap);

        DataCloudJobConfiguration jobConfiguration = new DataCloudJobConfiguration();
        jobConfiguration.setHdfsPodId(podId);
        jobConfiguration.setName("DataCloudMatchBlock");
        jobConfiguration.setCustomerSpace(CustomerSpace.parse("DCTest"));
        jobConfiguration.setAvroPath(avroPath);
        jobConfiguration.setBlockSize(AvroUtils.count(yarnConfiguration, avroPath).intValue());
        jobConfiguration.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setBlockOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setThreadPoolSize(4);
        jobConfiguration.setGroupSize(10);
        jobConfiguration.setMatchInput(matchInput);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "functional")
    public void testRTSNoMatch() throws Exception {
        String fileName = "BulkMatchInput_NoMatch.avro";
        cleanupAvroDir(avroDir);
        uploadDataCsv(avroDir, fileName);

        String avroPath = avroDir + "/" + fileName;
        String latestDataCloudVersion = versionEntityMgr.latestApprovedForMajorVersion(latestMajorVersion).getVersion();

        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(schema);

        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant("DCTest"));
        matchInput.setPredefinedSelection(Predefined.RTS);
        matchInput.setDataCloudVersion(latestDataCloudVersion);
        matchInput.setKeyMap(keyMap);

        DataCloudJobConfiguration jobConfiguration = new DataCloudJobConfiguration();
        jobConfiguration.setHdfsPodId(podId);
        jobConfiguration.setName("DataCloudMatchBlock");
        jobConfiguration.setCustomerSpace(CustomerSpace.parse("DCTest"));
        jobConfiguration.setAvroPath(avroPath);
        jobConfiguration.setBlockSize(AvroUtils.count(yarnConfiguration, avroPath).intValue());
        jobConfiguration.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setBlockOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setThreadPoolSize(4);
        jobConfiguration.setGroupSize(10);
        jobConfiguration.setMatchInput(matchInput);

        ApplicationId applicationId = dataCloudYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

}
