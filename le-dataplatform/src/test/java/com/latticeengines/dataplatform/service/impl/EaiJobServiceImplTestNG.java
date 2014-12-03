package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.runtime.eai.EaiContainerProperty;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.eai.EaiJobService;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.scheduler.exposed.fairscheduler.LedpQueueAssigner;

public class EaiJobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private EaiJobService eaiJobService;

    @Test(groups = { "functional", "functional.production" }, enabled = false)
    public void testSubmitEaiYarnJob() throws Exception {
        String metadata = FileUtils.readFileToString(new File("src/test/resources/testdata.json"));
        JSONParser parser = new JSONParser();
        JSONArray resultArr = (JSONArray) parser.parse(metadata);
        List<Table> tables = new ArrayList<>();
        for (Object obj : resultArr.toArray()) {
            tables.add(JsonUtils.deserialize(obj.toString(), Table.class));
        }
        String targetPath = "/tmp";
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "EaiTest");
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getNonMRQueueNameForSubmission(0));

        Properties containerProperties = new Properties();
        containerProperties.put(EaiContainerProperty.TABLES.name(), tables.toString());
        containerProperties.put(EaiContainerProperty.TARGET_PATH.name(), targetPath);
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), 1);
        containerProperties.put(ContainerProperty.MEMORY.name(), 128);
        containerProperties.put(ContainerProperty.PRIORITY.name(), 0);

        ApplicationId applicationId = eaiJobService
                .submitYarnJob("eaiClient", appMasterProperties, containerProperties);
        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String contextFileName = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
        String metadataFileName = containerProperties.getProperty(PythonContainerProperty.METADATA.name());

        assertFalse(new File(contextFileName).exists());
        assertFalse(new File(metadataFileName).exists());
    }
}
