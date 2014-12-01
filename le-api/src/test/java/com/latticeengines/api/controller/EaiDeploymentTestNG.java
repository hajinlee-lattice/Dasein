package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.testng.annotations.Test;

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiConfiguration;
import com.latticeengines.domain.exposed.eai.Table;

public class EaiDeploymentTestNG extends ApiFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(EaiDeploymentTestNG.class);

    private String customer = "Eai-" + suffix;

    @Test(groups = "deployment", enabled = true)
    public void invokeEai() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "eai");
        EaiConfiguration config = getLoadConfig();
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/eai", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    private EaiConfiguration getLoadConfig() throws IOException, ParseException {
        EaiConfiguration config = new EaiConfiguration();
        String metadata = FileUtils.readFileToString(new File("../le-dataplatform/src/test/resources/testdata.json"));
        JSONParser parser = new JSONParser();
        JSONArray resultArr = (JSONArray) parser.parse(metadata);
        List<Table> tables = new ArrayList<>();
        for (Object obj : resultArr.toArray()) {
            tables.add(JsonUtils.deserialize(obj.toString(), Table.class));
        }
        String targetPath = "/tmp";
        config.setCustomer(customer);
        config.setTables(tables);
        config.setTargetPath(targetPath);
        return config;
    }
}
