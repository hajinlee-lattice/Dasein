package com.latticeengines.spark.exposed.job.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.graph.ConvertToGraphJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class ConvertToGraphJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ConvertToGraphJobTestNG.class);

    private static final String ENTITY = "Entity";
    private static final String TEMPLATE_ID = "TemplateID";
    private static final String MATCH_IDS = "MatchIDs";
    private static final String UNIQUE_ID = "UniqueID";

    private static final List<Pair<String, Class<?>>> INPUT_FIELDS = Arrays.asList( //
            Pair.of("AccountID", String.class), //
            Pair.of("Domain", String.class), //
            Pair.of("SystemName", String.class), //
            Pair.of("SalesforceAccountID", String.class), //
            Pair.of("DW1AccountID", String.class), //
            Pair.of("DW2AccountID", String.class));

    private List<String> inputs = new ArrayList<>();
    private List<Map<String, String>> inputDescriptors = new ArrayList<>();

    @Test(groups = "functional")
    public void runTest() {
        prepareData();
        ConvertToGraphJobConfig config = new ConvertToGraphJobConfig();
        config.setInputDescriptors(inputDescriptors);

        SparkJobResult result = runSparkJob(ConvertToGraphJob.class, config, inputs, getWorkspace());
        log.info("Result = {}", JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);

        // Vertices: {Input1: DocV 2, IdV 2}, {Input2: DocV 2, IdV 4}, {Input3: DocV 1, IdV 2}
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 13);

        // Edges: {Input1: 2}, {Input2: 4}, {Input3: 2}
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 8);
    }

    private void prepareData() {
        log.info("Preparing inputs...");

        Object[][] accountInput1 = new Object[][] { //
            { "A10001", "abc.com", "Salesforce", "SF000022", null, null }, //
            { "A10002", "aaa.com", "Salesforce", "SF000045", null, null }, //
        };
        inputs.add(uploadHdfsDataUnit(accountInput1, INPUT_FIELDS));

        Object[][] accountInput2 = new Object[][] { //
            { "A10003", "aba.com", "DW1", "SF000022", "DW1000043", null }, //
            { "A10004", "bnb.com", "DW1", "SF000023", "DW1000098", null }, //
        };
        inputs.add(uploadHdfsDataUnit(accountInput2, INPUT_FIELDS));

        Object[][] accountInput3 = new Object[][] { //
            { "A10005", "cad.com", "DW2", "SF000045", "DW1000098", "DW2000011" }, //
        };
        inputs.add(uploadHdfsDataUnit(accountInput3, INPUT_FIELDS));

        Map<String, String> descriptor1 = new HashMap<>();
        descriptor1.put(ENTITY, "Account");
        descriptor1.put(TEMPLATE_ID, "SalesforceAccountID");
        descriptor1.put(MATCH_IDS, "SalesforceAccountID");
        descriptor1.put(UNIQUE_ID, "SF0001");

        Map<String, String> descriptor2 = new HashMap<>();
        descriptor2.put(ENTITY, "Account");
        descriptor2.put(TEMPLATE_ID, "DW1AccountID");
        descriptor2.put(MATCH_IDS, "SalesforceAccountID,Domain");
        descriptor2.put(UNIQUE_ID, "DW10001");

        Map<String, String> descriptor3 = new HashMap<>();
        descriptor3.put(ENTITY, "Account");
        descriptor3.put(TEMPLATE_ID, "DW2AccountID");
        descriptor3.put(MATCH_IDS, "DW1AccountID,SalesforceAccountID");
        descriptor3.put(UNIQUE_ID, "DW20001");

        inputDescriptors.add(descriptor1);
        inputDescriptors.add(descriptor2);
        inputDescriptors.add(descriptor3);
    }
}
