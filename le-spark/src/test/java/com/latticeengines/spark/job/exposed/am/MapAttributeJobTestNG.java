package com.latticeengines.spark.job.exposed.am;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig;
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig.JoinConfig;
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig.JoinTarget;
import com.latticeengines.domain.exposed.spark.am.MapAttributeTxfmrConfig.MapFunc;
import com.latticeengines.spark.exposed.job.am.MapAttributeJob;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class MapAttributeJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testDnBAccountMasterRefresh() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(this::testDnBAmRefresh);
        ThreadPoolUtils.runInParallel(this.getClass().getSimpleName(), runnables);
    }

    private void testDnBAmRefresh() {
        List<String> input = upload2Data();
        MapAttributeTxfmrConfig config = getConfigForDnBAccountMasterRefresh();
        SparkJobResult result = runSparkJob(MapAttributeJob.class, config, input,
                String.format("/tmp/%s/%s/dnbAccountMasterRefresh", leStack, this.getClass().getSimpleName()));
        verify(result, Collections.singletonList(this::verifyDnBAmRefresh));
    }

    private List<String> upload1Data() {
        List<String> input = new ArrayList<>();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("LatticeID", String.class), //
                Pair.of("LDC_DUNS", String.class), //
                Pair.of("LDC_Domain", String.class), //
                Pair.of("LDC_Street", String.class), //
                Pair.of("LDC_State", String.class), //
                Pair.of("LDC_Country", String.class) //
        );
        Object[][] data = getInput1Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private List<String> upload2Data() {
        List<String> input = upload1Data();
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("Id", Integer.class), //
                Pair.of("DUNS_NUMBER", String.class), //
                Pair.of("LE_DOMAIN", String.class), //
                Pair.of("LE_STREET", String.class), //
                Pair.of("LE_STATE", String.class), //
                Pair.of("LE_COUNTRY", String.class) //

        );
        Object[][] data = getInput2Data();
        input.add(uploadHdfsDataUnit(data, fields));
        return input;
    }

    private Object[][] getInput1Data() {
        return new Object[][] { //
                { 1, "1", "duns1", "domain1", "street11", "state11", "country11" }, //
                { 2, "2", "duns2", null, "street12", "state12", "country12" }, //
                { 3, "3", "duns3", null, "street13", "state13", "country13" }, //
                { 4, "4", null, "domain2", "street14", "state14", "country14" }, //
                { 5, "5", null, null, "street15", "state15", "country15" }, //
                { 6, "6", "duns_nomatch", "domain6", "street16", "state16", "country16" }, //
                { 7, "7", "duns_same", "domain_same", "street17", "state17", "country17" }, //
        };
    }

    private Object[][] getInput2Data() {
        return new Object[][] { //
                { 1, "duns1", "domain1", "street21", "state21", "country21" }, //
                { 2, "duns2", null, "street22", "state22", "country22" }, //
                { 3, "duns3", null, "street23", "state23", "country23" }, //
                { 4, null, "domain2", "street24", "state24", "country24" }, //
                { 5, null, null, "street25", "state25", "country25" }, //
                { 6, "duns_nomatch2", "domain6", "street26", "state26", "countr26" }, //
                { 7, "duns_same", "domain_same", "street17", "state17", "countr27" }, //
        };
    }

    private MapAttributeTxfmrConfig getConfigForDnBAccountMasterRefresh() {
        MapAttributeTxfmrConfig config = new MapAttributeTxfmrConfig();
        config.setSource("AccountMaster");
        config.setStage("DnBRefreshStage");
        config.setTemplates(Arrays.asList("AccountMaster", "DnBCacheSeedRaw"));
        config.setBaseTables(Arrays.asList("AccountMaster", "DnBCacheSeedRaw"));
        config.setSeed("AccountMaster");
        config.setSeedId("LatticeID");
        config.setIsDedupe(false);

        List<SourceAttribute> srcAttrs = new ArrayList<>();
        SourceAttribute srcAttr = new SourceAttribute();
        srcAttr.setArguments("{\"target\":null,\"attribute\":\"LE_STREET\",\"Source\":\"DnBCacheSeedRaw\"}");
        srcAttr.setAttribute("LDC_Street");
        srcAttr.setSource("AccountMaster");
        srcAttr.setStage("DnBRefreshStage");
        srcAttr.setTransformer("mapAttribute");
        srcAttrs.add(srcAttr);

        srcAttr = new SourceAttribute();
        srcAttr.setArguments("{\"target\":null,\"attribute\":\"LE_STATE\",\"Source\":\"DnBCacheSeedRaw\"}");
        srcAttr.setAttribute("LDC_State");
        srcAttr.setSource("AccountMaster");
        srcAttr.setStage("DnBRefreshStage");
        srcAttr.setTransformer("mapAttribute");
        srcAttrs.add(srcAttr);
        config.setSrcAttrs(srcAttrs);

        List<MapFunc> mapFuncs = new ArrayList<>();
        MapFunc mapFunc = new MapFunc();
        mapFunc.setAttribute("LE_STREET");
        mapFunc.setSource("DnBCacheSeedRaw");
        mapFunc.setTarget("LDC_Street");
        mapFuncs.add(mapFunc);
        mapFunc = new MapFunc();
        mapFunc.setAttribute("LE_STATE");
        mapFunc.setSource("DnBCacheSeedRaw");
        mapFunc.setTarget("LDC_State");
        mapFuncs.add(mapFunc);
        config.setMapFuncs(mapFuncs);

        List<JoinConfig> joinConfigs = new ArrayList<>();
        JoinConfig joinConfig = new JoinConfig();
        joinConfig.setKeys(Arrays.asList("LDC_DUNS", "LDC_Domain"));
        List<JoinTarget> targets = new ArrayList<>();
        JoinTarget target = new JoinTarget();
        target.setKeys(Arrays.asList("DUNS_NUMBER", "LE_DOMAIN"));
        target.setSource("DnBCacheSeedRaw");
        targets.add(target);
        joinConfig.setTargets(targets);
        joinConfigs.add(joinConfig);
        config.setJoinConfigs(joinConfigs);

        return config;
    }

    private Boolean verifyDnBAmRefresh(HdfsDataUnit tgt) {
        Iterator<GenericRecord> iter = verifyAndReadTarget(tgt);
        int rows = 0;
        for (GenericRecord record : (Iterable<GenericRecord>) () -> iter) {
            Assert.assertEquals(record.getSchema().getFields().size(), 7, record.toString());

            String id = record.get("Id").toString();
            String latticeID = record.get("LatticeID") != null ? record.get("LatticeID").toString() : null;
            String duns = record.get("LDC_DUNS") != null ? record.get("LDC_DUNS").toString() : null;
            String domain = record.get("LDC_Domain") != null ? record.get("LDC_Domain").toString() : null;
            String street = record.get("LDC_Street") != null ? record.get("LDC_Street").toString() : null;
            String state = record.get("LDC_State") != null ? record.get("LDC_State").toString() : null;
            String country = record.get("LDC_Country") != null ? record.get("LDC_Country").toString() : null;
            switch (id) {
            case "1":
                Assert.assertEquals(latticeID, "1", record.toString());
                Assert.assertEquals(duns, "duns1", record.toString());
                Assert.assertEquals(domain, "domain1", record.toString());
                Assert.assertEquals(street, "street21", record.toString());
                Assert.assertEquals(state, "state21", record.toString());
                Assert.assertEquals(country, "country11", record.toString());
                break;
            case "2":
                Assert.assertEquals(latticeID, "2", record.toString());
                Assert.assertEquals(duns, "duns2", record.toString());
                Assert.assertNull(domain, record.toString());
                Assert.assertEquals(street, "street22", record.toString());
                Assert.assertEquals(state, "state22", record.toString());
                Assert.assertEquals(country, "country12", record.toString());
                break;
            case "3":
                Assert.assertEquals(latticeID, "3", record.toString());
                Assert.assertEquals(duns, "duns3", record.toString());
                Assert.assertNull(domain, record.toString());
                Assert.assertEquals(street, "street23", record.toString());
                Assert.assertEquals(state, "state23", record.toString());
                Assert.assertEquals(country, "country13", record.toString());
                break;
            case "4":
                Assert.assertEquals(latticeID, "4", record.toString());
                Assert.assertNull(duns, record.toString());
                Assert.assertEquals(domain, "domain2", record.toString());
                Assert.assertEquals(street, "street24", record.toString());
                Assert.assertEquals(state, "state24", record.toString());
                Assert.assertEquals(country, "country14", record.toString());
                break;
            case "5":
                Assert.assertEquals(latticeID, "5", record.toString());
                Assert.assertNull(duns, record.toString());
                Assert.assertNull(domain, record.toString());
                Assert.assertNull(street, record.toString());
                Assert.assertNull(state, record.toString());
                Assert.assertEquals(country, "country15", record.toString());

                break;
            case "6":
                Assert.assertEquals(latticeID, "6", record.toString());
                Assert.assertEquals(duns, "duns_nomatch", record.toString());
                Assert.assertEquals(domain, "domain6", record.toString());
                // No-matched records, whose replaced columns are null, need to
                // be filtered out in next step.
                Assert.assertNull(street, record.toString());
                Assert.assertNull(state, record.toString());
                Assert.assertEquals(country, "country16", record.toString());

                break;
            case "7":
                Assert.assertEquals(latticeID, "7", record.toString());
                Assert.assertEquals(duns, "duns_same", record.toString());
                Assert.assertEquals(domain, "domain_same", record.toString());
                // no changed records, whose replaced columns are null, need to
                // be filtered out in next step.
                Assert.assertNull(street, record.toString());
                Assert.assertNull(state, record.toString());
                Assert.assertEquals(country, "country17", record.toString());

                break;
            default:
            }
            rows++;
        }
        Assert.assertEquals(rows, 7);
        return true;
    }

}
