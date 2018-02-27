package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloudapi.engine.purge.service.PurgeService;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;

@Component
public class PurgeServiceImplTestNG extends PropDataEngineFunctionalTestNGBase {
    public final String POD_ID = this.getClass().getSimpleName();

    @SuppressWarnings("unused")
    private static Logger log = LoggerFactory.getLogger(PurgeServiceImplTestNG.class);

    @Autowired
    private PurgeService purgeService;

    private PurgeSource pipelineTempSourceToPurge;
    private PurgeSource operationalSourceToPurge;

    private Map<String, PurgeSource> validationMapNonDebugMode;
    private Map<String, PurgeSource> validationMapDebugMode;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        preparePipelineTempSource();
        prepareOperationalSourceToPurge();
        prepareValidationMap();
    }

    @Test(groups = "functional")
    public void testScan() throws IOException {
        List<PurgeSource> toPurge = purgeService.scan(POD_ID, false);
        toPurge.forEach(purgeSrc -> System.out.println(JsonUtils.serialize(purgeSrc)));
        validatePurgeSourcesNonDebugMode(toPurge);

        toPurge = purgeService.scan(POD_ID, true);
        toPurge.forEach(purgeSrc -> System.out.println(JsonUtils.serialize(purgeSrc)));
        validatePurgeSourcesDebugMode(toPurge);
    }

    private void preparePipelineTempSource() throws IOException {
        String srcName = "Pipeline_AccountMasterSeedClean_version_2018-01-10_05-41-36_UTC_step_1";
        String hdfsPath = hdfsPathBuilder.constructSourceBaseDir().append(srcName).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPath);
        pipelineTempSourceToPurge = new PurgeSource(srcName, null, hdfsPaths, null, false);
    }

    private void prepareOperationalSourceToPurge() throws IOException {
        String srcName = "LDCDEV_SuspectRecords";
        String hdfsPath = hdfsPathBuilder.constructSourceBaseDir().append(srcName).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPath);
        operationalSourceToPurge = new PurgeSource(srcName, null, hdfsPaths, null, false);
    }

    private void prepareValidationMap() {
        validationMapNonDebugMode = new HashMap<>();

        validationMapDebugMode = new HashMap<>();
        validationMapDebugMode.put(getValidationKey(pipelineTempSourceToPurge), pipelineTempSourceToPurge);
        validationMapDebugMode.put(getValidationKey(operationalSourceToPurge), operationalSourceToPurge);
    }

    private String getValidationKey(PurgeSource purgeSource) {
        return String.valueOf(purgeSource.getSourceName()) + "_" + String.valueOf(purgeSource.getVersion());
    }

    private void validatePurgeSourcesNonDebugMode(List<PurgeSource> toPurge) {
        Assert.assertEquals(0, toPurge.size());
    }

    private void validatePurgeSourcesDebugMode(List<PurgeSource> toPurge) {
        Assert.assertEquals(2, toPurge.size());
        toPurge.forEach(purgeSource -> {
            PurgeSource expected = validationMapDebugMode.get(getValidationKey(purgeSource));
            Assert.assertNotNull(expected);
            Assert.assertTrue(isValidatedList(expected.getHdfsPaths(), purgeSource.getHdfsPaths()));
            Assert.assertTrue(isValidatedList(expected.getHiveTables(), purgeSource.getHiveTables()));
        });
    }

    private boolean isValidatedList(List<String> expected, List<String> actual) {
        if (expected == null && actual == null) {
            return true;
        }
        if (expected == null || actual == null) {
            return false;
        }
        if (expected.size() != actual.size()) {
            return false;
        }
        Set<String> expectedSet = new HashSet<>(expected);
        for (String ent : actual) {
            if (!expectedSet.contains(ent)) {
                return false;
            }
        }
        return true;
    }
}
