package com.latticeengines.spark.exposed.job.cdl;

import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LDC_Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LastModifiedDate;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ModelName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ModelNameId;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateIntentAlertArtifactsConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateIntentAlertArtifactsJobTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateIntentAlertArtifactsJobTestNG.class);

    private static final List<Pair<String, Class<?>>> LATTICEACCOUNT_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(LDC_Name.name(), String.class), //
            Pair.of("LDC_PrimaryIndustry", String.class), //
            Pair.of("LDC_City", String.class), //
            Pair.of("LDC_Domain", String.class), //
            Pair.of("LDC_DUNS", String.class));

    private static final List<Pair<String, Class<?>>> RAWSTREAM_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of(ModelName.name(), String.class), //
            Pair.of(LastModifiedDate.name(), Long.class), //
            Pair.of("DUNS", String.class));

    // Intent by model with last 1, 2, 4, 8, 12 weeks
    private static final List<Pair<String, Class<?>>> METRICSGROUP_IBM_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of("am_ibm__1__w_1_w", Boolean.class), //
            Pair.of("am_ibm__2__w_2_w", Boolean.class), //
            Pair.of("am_ibm__3__w_4_w", Boolean.class), //
            Pair.of("am_ibm__4__w_8_w", Boolean.class), //
            Pair.of("am_ibm__5__w_12_w", Boolean.class));

    // Buying stage by model, every week
    private static final List<Pair<String, Class<?>>> METRICSGROUP_BSBM_FIELDS = Arrays.asList( //
            Pair.of(AccountId.name(), String.class), //
            Pair.of("am_bsbm__1__ev_w", String.class), //
            Pair.of("am_bsbm__2__ev_w", String.class), //
            Pair.of("am_bsbm__3__ev_w", String.class), //
            Pair.of("am_bsbm__4__ev_w", String.class), //
            Pair.of("am_bsbm__5__ev_w", String.class));

    private static final List<String> SELECTED_ATTRIBUTES = ImmutableList.of( //
            AccountId.name(), //
            "ModelName", //
            "LDC_PrimaryIndustry", //
            "Stage", //
            LDC_Name.name(), //
            "LDC_DUNS" //
    );

    List<String> inputs = new ArrayList<>();
    DimensionMetadata dimensionMetadata = null;

    @Test(groups = "functional")
    private void test() {
        prepareData();

        GenerateIntentAlertArtifactsConfig config = new GenerateIntentAlertArtifactsConfig();
        config.setDimensionMetadata(dimensionMetadata);
        config.setSelectedAttributes(SELECTED_ATTRIBUTES);

        SparkJobResult result = runSparkJob(GenerateIntentAlertArtifactsJob.class, config, inputs,
                String.format("/tmp/%s/%s/GenerateIntentAlertArtifactsJob", leStack, this.getClass().getSimpleName()));
        log.info("Result = {}", JsonUtils.serialize(result));
        verify(result, Arrays.asList(this::verifyNewAccounts, this::verifyAllAccounts));
    }

    private void prepareData() {
        Object[][] latticeAccountData = new Object[][] { //
                { "01f0iorkcvc39gpj", "Morgan Stanley", "Finance", "San Francisco", "www.morganstanley.com",
                        "039741717" }, //
                { "034nxyxyfz2uwyw1", "Facebook", "Internet", "Menlo Park", "www.fb.com", "160733585" }, //
                { "046vxci0yxv0mo8n", "Costco", "Grocery", "San Meto", "www.costco.com", "114462769" }, //
                { "04wis14pzgqzxxd0", "Cheesecake Factory", "Restaurant", "Mountainview",
                        "www.thecheesecakefactory.com", "082339268" }, //
        };
        inputs.add(uploadHdfsDataUnit(latticeAccountData, LATTICEACCOUNT_FIELDS));

        Object[][] rawStreamData = new Object[][] { //
                { "01f0iorkcvc39gpj", "DNBinternal-intent", 1601831247000L, "039741717" }, //
                { "01f0iorkcvc39gpj", "DNBinternal-optimizer", 1601931247000L, "039741717" }, //
                { "01f0iorkcvc39gpj", "DNBinternal-optimizer", 1601758447000L, "039741717" }, // same model, earlier
                                                                                              // date
                { "034nxyxyfz2uwyw1", "DNBinternal-optimizer", 1601585647000L, "160733585" }, //
                { "034nxyxyfz2uwyw1", "DNBinternal-ABM", 1601485777000L, "160733585" }, //
                { "046vxci0yxv0mo8n", "DNBinternal-intent", 1601499247000L, "114462769" }, //
                { "04wis14pzgqzxxd0", "DNBinternal-optimizer", 1601326447000L, "082339268" }, //
                { "04wis14pzgqzxxd0", "DNBinternal-ABM", 1603126888000L, "082339268" }, //
                { "04wis14pzgqzxxd0", "DNBinternal-ABM", 1603004888000L, "082339268" }, // same model, earlier date
        };
        inputs.add(uploadHdfsDataUnit(rawStreamData, RAWSTREAM_FIELDS));

        Object[][] ibmData = new Object[][] { //
                { "01f0iorkcvc39gpj", false, false, false, true, true }, //
                { "034nxyxyfz2uwyw1", false, false, false, false, true }, //
                { "046vxci0yxv0mo8n", true, true, true, true, true }, // account shown in last week
                { "04wis14pzgqzxxd0", false, false, true, true, true }, //
        };
        inputs.add(uploadHdfsDataUnit(ibmData, METRICSGROUP_IBM_FIELDS));

        Object[][] bsbmData = new Object[][] { //
                { "01f0iorkcvc39gpj", null, "Buying", "Researching", null, null }, //
                { "034nxyxyfz2uwyw1", null, "Buying", null, "Buying", null }, //
                { "046vxci0yxv0mo8n", null, null, "Researching", null, null }, //
                { "04wis14pzgqzxxd0", null, "Researching", null, "Buying", null }, // multiple results from different
                                                                                   // models
        };
        inputs.add(uploadHdfsDataUnit(bsbmData, METRICSGROUP_BSBM_FIELDS));

        dimensionMetadata = new DimensionMetadata();
        dimensionMetadata.setDimensionValues(Arrays.asList( //
                intentModelValue("1", "DNBinternal-hoovers"), //
                intentModelValue("2", "DNBinternal-optimizer"), //
                intentModelValue("3", "DNBinternal-intent"), //
                intentModelValue("4", "DNBinternal-ABM"), //
                intentModelValue("5", "D&B_ABM_intent_model")));
        dimensionMetadata.setCardinality(5);
    }

    private Map<String, Object> intentModelValue(String modelNameId, String modelName) {
        Map<String, Object> values = new HashMap<>();
        values.put(ModelName.name(), modelName);
        values.put(ModelNameId.name(), modelNameId);
        return values;
    }

    private Boolean verifyNewAccounts(HdfsDataUnit output1) {
        Object[][] expectedResult = new Object[][] {
                { "01f0iorkcvc39gpj", "DNBinternal-optimizer", "Finance", "Buying", "Morgan Stanley", "039741717" },
                { "01f0iorkcvc39gpj", "DNBinternal-intent", "Finance", "Researching", "Morgan Stanley", "039741717" },
                { "034nxyxyfz2uwyw1", "DNBinternal-optimizer", "Internet", "Buying", "Facebook", "160733585" },
                { "034nxyxyfz2uwyw1", "DNBinternal-ABM", "Internet", "Buying", "Facebook", "160733585" },
                { "04wis14pzgqzxxd0", "DNBinternal-optimizer", "Restaurant", "Researching", "Cheesecake Factory",
                        "082339268" },
                { "04wis14pzgqzxxd0", "DNBinternal-ABM", "Restaurant", "Buying", "Cheesecake Factory", "082339268" } };

        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult).collect(Collectors
                .toMap(arr -> arr[4].toString() + "_" + arr[1].toString() + "_" + arr[3].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(output1);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            String id = record.get("CompanyName").toString() + "_" + record.get(ModelName.name()).toString() + "_"
                    + record.get("Stage").toString();
            verifyTargetData(expectedMap, record, id);
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);

        return true;
    }

    private Boolean verifyAllAccounts(HdfsDataUnit output2) {
        Object[][] expectedResult = new Object[][] {
                { "DNBinternal-optimizer", "Finance", "Buying", "Morgan Stanley", "039741717", "2020-10-03" },
                { "DNBinternal-intent", "Finance", "Researching", "Morgan Stanley", "039741717", "2020-10-04" },
                { "DNBinternal-optimizer", "Internet", "Buying", "Facebook", "160733585", "2020-10-01" },
                { "DNBinternal-ABM", "Internet", "Buying", "Facebook", "160733585", "2020-09-30" },
                { "DNBinternal-intent", "Grocery", "Researching", "Costco", "114462769", "2020-09-30" },
                { "DNBinternal-optimizer", "Restaurant", "Researching", "Cheesecake Factory", "082339268",
                        "2020-09-28" },
                { "DNBinternal-ABM", "Restaurant", "Buying", "Cheesecake Factory", "082339268", "2020-10-18" } };

        Map<Object, List<Object>> expectedMap = Arrays.stream(expectedResult).collect(Collectors
                .toMap(arr -> arr[3].toString() + "_" + arr[0].toString() + "_" + arr[2].toString(), Arrays::asList));
        Iterator<GenericRecord> iterator = verifyAndReadTarget(output2);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iterator) {
            String id = record.get("CompanyName").toString() + "_" + record.get(ModelName.name()).toString() + "_"
                    + record.get("Stage").toString();
            verifyTargetData(expectedMap, record, id);
            rows++;
        }
        Assert.assertEquals(rows, expectedResult.length);

        return true;
    }

    private void verifyTargetData(Map<Object, List<Object>> expectedMap, GenericRecord record, String id) {
        log.info("record: {}, id: {}", record.toString(), id);
        Assert.assertNotNull(record);
        log.info("expectedMap: {}", expectedMap);
        Assert.assertNotNull(expectedMap.get(id));
        List<Object> actual = record.getSchema().getFields().stream()
                .map(field -> record.get(field.name()) != null ? record.get(field.name()).toString() : null)
                .collect(Collectors.toList());
        Assert.assertEquals(actual, expectedMap.get(id).stream().map(obj -> obj != null ? obj.toString() : null)
                .collect(Collectors.toList()));
    }
}
