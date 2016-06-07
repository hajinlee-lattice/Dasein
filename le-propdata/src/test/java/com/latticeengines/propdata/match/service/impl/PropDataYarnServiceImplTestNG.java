package com.latticeengines.propdata.match.service.impl;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.PropDataJobConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyUtils;
import com.latticeengines.propdata.match.service.PropDataYarnService;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;


@Component
public class PropDataYarnServiceImplTestNG extends PropDataMatchFunctionalTestNGBase {

    private static final String avroDir = "/tmp/PropDataYarnServiceTestNG";
    private static final String fileName = "BulkMatchInput.avro";
    private static final String podId = "PropDataYarnServiceImplTestNG";

    @Autowired
    private PropDataYarnService propDataYarnService;

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

        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroPath));
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(schema);

        PropDataJobConfiguration jobConfiguration = new PropDataJobConfiguration();
        jobConfiguration.setHdfsPodId(podId);
        jobConfiguration.setReturnUnmatched(true);
        jobConfiguration.setName("PropDataMatchBlock");
        jobConfiguration.setCustomerSpace(CustomerSpace.parse("PDTest"));
        jobConfiguration.setAvroPath(avroPath);
        jobConfiguration.setPredefinedSelection(ColumnSelection.Predefined.DerivedColumns);
        jobConfiguration.setKeyMap(keyMap);
        jobConfiguration.setBlockSize(AvroUtils.count(yarnConfiguration, avroPath).intValue());
        jobConfiguration.setRootOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setBlockOperationUid(UUID.randomUUID().toString().toUpperCase());
        jobConfiguration.setThreadPoolSize(4);
        jobConfiguration.setGroupSize(10);

        ApplicationId applicationId = propDataYarnService.submitPropDataJob(jobConfiguration);
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, applicationId);
        Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

}
