package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.service.EaiService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.eai.EaiConfiguration;
import com.latticeengines.domain.exposed.eai.Table;

@Transactional
public class EaiServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private EaiService eaiService;

    protected static final Log log = LogFactory.getLog(EaiServiceImplTestNG.class);

    private String customer = "Eai-" + suffix;

    @Test(groups = { "functional", "functional.production" }, enabled = true)
    public void invokeEai() throws Exception {
        String metadata = FileUtils.readFileToString(new File(
                "src/test/resources/testdata.json"));
        JSONParser parser = new JSONParser();
        JSONArray resultArr = (JSONArray) parser.parse(metadata);
        List<Table> tables = new ArrayList<>();
        for (Object obj : resultArr.toArray()) {
            tables.add(JsonUtils.deserialize(obj.toString(), Table.class));
        }
        String targetPath = "/tmp";
        EaiConfiguration eaiConfig = new EaiConfiguration();
        eaiConfig.setCustomer(customer);
        eaiConfig.setTables(tables);
        eaiConfig.setTargetPath(targetPath);
        ApplicationId appId = eaiService.invokeEai(eaiConfig);
        assertNotNull(appId);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }
}
