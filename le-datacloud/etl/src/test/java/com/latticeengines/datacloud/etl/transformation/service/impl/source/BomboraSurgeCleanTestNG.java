package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.BomboraSurgeCleanFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BomboraSurgeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;


public class BomboraSurgeCleanServiceTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(BomboraSurgeCleanServiceTestNG.class);

    GeneralSource source = new GeneralSource("BomboraSurge");
    GeneralSource bomboraSurgeRaw = new GeneralSource("BomboraSurgeRaw");

    String targetSourceName = "BomboraSurge";

    ObjectMapper om = new ObjectMapper();

    @Test(groups = "functional")
    public void testTransformation() {
        prepareBomboraSurgeRaw();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("BomboraSurge");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(bomboraSurgeRaw.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(BomboraSurgeCleanFlow.TRANSFORMER_NAME);
            step1.setTargetSource(targetSourceName);
            String confParamStr1 = getTransformerConfig();
            step1.setConfiguration(confParamStr1);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getTransformerConfig() throws JsonProcessingException {
        BomboraSurgeConfig config = new BomboraSurgeConfig();
        config.setMetroAreaField("MetroArea");
        config.setDomainOriginField("DomainOrigin");
        config.setCountryField("Country");
        config.setStateField("State");
        config.setCityField("City");
        return om.writeValueAsString(config);
    }

    private Object[][] data = new Object[][] { //
            { 1, "universidade federal fluminense", null, "Brazil" }, //
            { 2, "websense", "Sydney (Baulkham Hills / Hawkesbury), NSW", "USA" }, //
            { 3, "rafael advanced defense systems", "Barossa / Yorke / Mid North, SA", "USA" }, //
            { 4, "adm interactive oü", "Aba(Ngawa) Tibetan And Qiang Autonomous Prefecture, CN-51", "China" }, //
            { 5, "the atlanta journal-constitution", "Idahoe Falls / Pocatello, ID / Jackson, WY", "USA" }, //
            { 6, "Deatnu Tana", null, null }, //
            { 7, "Edis", "Foster City", null }, //
            { 8, "university of sunderland", "Foster City", "USA" }, //
    };

    private Object[][] expectedData = new Object[][] { //
            { 1, "universidade federal fluminense", null, null, "Brazil" }, //
            { 2, "websense", "Sydney", "NSW", "Australia" }, //
            { 3, "rafael advanced defense systems", "Barossa", "SA", "Australia" }, //
            { 3, "rafael advanced defense systems", "Yorke", "SA", "Australia" }, //
            { 3, "rafael advanced defense systems", "Mid North", "SA", "Australia" }, //
            { 4, "adm interactive oü", "Aba Tibetan And Qiang Autonomous Prefecture", null, "China" }, //
            { 5, "the atlanta journal-constitution", "Idahoe Falls", "ID", "United States" }, //
            { 5, "the atlanta journal-constitution", "Pocatello", "ID", "United States" }, //
            { 5, "the atlanta journal-constitution", "Jackson", "WY", "United States" }, //
            { 6, "Deatnu Tana", null, null, null }, //
            { 7, "Edis", "Foster City", null, null }, //
            { 8, "university of sunderland", "Foster City", null, "USA" }, //
    };

    private void prepareBomboraSurgeRaw() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("Company", String.class));
        columns.add(Pair.of("MetroArea", String.class));
        columns.add(Pair.of("DomainOrigin", String.class));
        uploadBaseSourceData(bomboraSurgeRaw.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        Map<Integer, List<NameLocation>> loc = new HashMap<>();
        for (Object[] data : expectedData) {
            if (!loc.containsKey(data[0])) {
                loc.put((Integer) data[0], new ArrayList<NameLocation>());
            }
            NameLocation nl = new NameLocation();
            nl.setName((String) data[1]);
            nl.setCity((String) data[2]);
            nl.setState((String) data[3]);
            nl.setCountry((String) data[4]);
            loc.get((Integer) data[0]).add(nl);
        }
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Integer id = (Integer) record.get("ID");
            Assert.assertTrue(loc.containsKey(id));
            List<NameLocation> nls = loc.get(id);
            boolean found = false;
            for (NameLocation nl : nls) {
                Object city = record.get("City");
                if (city instanceof Utf8) {
                    city = city.toString();
                }
                Object state = record.get("State");
                if (state instanceof Utf8) {
                    state = state.toString();
                }
                Object country = record.get("Country");
                if (country instanceof Utf8) {
                    country = country.toString();
                }
                if (isEqual(city, nl.getCity()) && isEqual(state, nl.getState()) && isEqual(country, nl.getCountry())) {
                    found = true;
                    break;
                }
            }
            Assert.assertTrue(found);
            rowNum++;
        }
        Assert.assertEquals(12, rowNum);
    }

    private boolean isEqual(Object obj1, Object obj2) {
        log.info(String.format("Obj1: %s, Obj2: %s", obj1, obj2));
        if (obj1 == null && obj2 == null) {
            return true;
        }
        if (obj1 != null && obj2 != null && obj1.equals(obj2)) {
            return true;
        }
        return false;
    }
}
