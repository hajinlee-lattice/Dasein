package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.core.entitymgr.SourceAttributeEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.MapAttributeFlow;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DeriveAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.MapAttributeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class TblDrivenTransformationTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(TblDrivenTransformationTestNG.class);

    private static final String ATTRIBUTE = "Attribute";
    private static final String DERIVED_ATTRIBUTE = "DerivedAttribute";

    private String targetSourceName = "DerivedSource";

    private String[] sourceTypes;
    private GeneralSource[][] sources;
    @SuppressWarnings("unused")
    private int[] numSources;
    private int numRecords;
    private String[][] keys;
    private int totalAttributes;

    @Autowired
    protected SourceAttributeEntityMgr sourceAttributeEntityMgr;


    ObjectMapper om = new ObjectMapper();

    @Test(groups = "pipeline1", enabled = false)
    public void testTransformation() {

        prepareSources();

        prepareMapTable();
        prepareDeriveTable();


        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareSources() {

        sourceTypes = new String[]{"Seed", "Domain", "Duns", "DomainDuns"};
        numRecords = 5;
        int[] numSources = new int[]{1, 3, 1, 2};
        keys = new String[4][];
        keys[0] = new String[] {"LatticeID", "Domain", "Duns"};
        keys[1] = new String[] {"Domain"};
        keys[2] = new String[] {"Duns"};
        keys[3] = new String[] {"Domain", "Duns"};

        sources = new GeneralSource[sourceTypes.length][];
        for (int i = 0; i < sourceTypes.length; i++) {
            GeneralSource[] sourcesPerType = new GeneralSource[numSources[i]];
            for (int j = 0; j < numSources[i]; j++) {
                sourcesPerType[j] = prepareSource(sourceTypes[i], keys[i], j, numRecords);
            }
            sources[i] = sourcesPerType;
        }
    }

    private GeneralSource prepareSource(String sourceType, String[] keys, int idx, int numRecords) {
        GeneralSource source = new GeneralSource(sourceType + idx);
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        for (int i = 0; i < keys.length; i++) {
            columns.add(Pair.of(keys[i], String.class));
        }
        columns.add(Pair.of(ATTRIBUTE, Long.class));

        Object[][] data = new Object[numRecords][keys.length + 1];

        for (int i = 0; i < numRecords; i++) {
            for (int j = 0; j < keys.length; j++) {
                data[i][j] = "" + i;
            }
            data[i][keys.length] = new Long(i);
        }
        uploadBaseSourceData(source.getSourceName(), baseSourceVersion, columns, data);

        return source;
    }

    private void prepareMapTable() {
        // Retain every key attribute from seed
        String[] seedKeys = keys[0];
        for (int i = 0; i < seedKeys.length; i++) {
            createMapAttribute(seedKeys[i], sourceTypes[0] + 0, seedKeys[i]);
        }

        // Collect remaining attrbute from all sources;
        int attrIdx = 0;
        for (int i = 0; i < sourceTypes.length; i++) {
            GeneralSource[] curSources = sources[i];
            for (int j = 0; j <  curSources.length; j++) {
                GeneralSource curSource = curSources[j];
                createMapAttribute(ATTRIBUTE + attrIdx, curSource.getSourceName(), ATTRIBUTE);
                attrIdx++;
            }
        }
        totalAttributes = attrIdx;
    }

    private void prepareDeriveTable() {
        List<String> origAttrs = new ArrayList<String>();
        for (int i = 0; i < totalAttributes; i++) {
            origAttrs.add(ATTRIBUTE + i);
        }
        DeriveAttributeConfig.DeriveFunc func = new DeriveAttributeConfig.DeriveFunc();
        func.setAttributes(origAttrs);
        func.setType(DeriveAttributeConfig.LONG_TYPE);
        func.setCalculation(DeriveAttributeConfig.SUM);
        try {
            String arguments = om.writeValueAsString(func);
            createSourceAttribute(targetSourceName, MapAttributeFlow.DERIVE_STAGE, MapAttributeFlow.DERIVE_TRANSFORMER,
                    DERIVED_ATTRIBUTE, arguments);
        } catch (Exception e) {
            log.error("Failed to create derived fuction", e);
        }
   
    }

    private void createMapAttribute(String attribute, String source, String origAttr) {
        MapAttributeConfig.MapFunc func = new MapAttributeConfig.MapFunc();
        func.setSource(source);
        func.setAttribute(origAttr);
        try {
            String arguments = om.writeValueAsString(func);
            createSourceAttribute(targetSourceName, MapAttributeFlow.MAP_STAGE, MapAttributeFlow.MAP_TRANSFORMER,
                    attribute, arguments);
        } catch (Exception e) {
            log.error("Failed to create derived fuction", e);
        }
    }

    private void createSourceAttribute(String source, String stage, String transformer, String attribute, String arguments) {
        SourceAttribute attr = new SourceAttribute();
        attr.setSource(source);
        attr.setStage(stage);
        attr.setTransformer(transformer);
        attr.setAttribute(attribute);
        attr.setArguments(arguments);

        try {
            sourceAttributeEntityMgr.createAttribute(attr);
        } catch (Exception e) {
            log.error("Skip already existing attribute ", e);
        }
     }
        

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("TblDrivenTransform");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();

            for (int i = 0; i < sources.length; i++) {
                for (int j = 0; j < sources[i].length; j++) {
                    baseSources.add(sources[i][j].getSourceName());
                }
            }

            step1.setBaseSources(baseSources);
            step1.setTransformer(MapAttributeFlow.MAP_TRANSFORMER);
            String confParamStr1 = getMapperConfig(baseSources);
            step1.setConfiguration(confParamStr1);

            TransformationStepConfig step2 = new TransformationStepConfig();
            List<String> templates = new ArrayList<String>();
            templates.add(targetSourceName);
            List<Integer> inputSteps = new ArrayList<>();
            inputSteps.add(0);
        
            step2.setInputSteps(inputSteps);
            step2.setTransformer(MapAttributeFlow.DERIVE_TRANSFORMER);
            step2.setTargetSource(targetSourceName);
            String confParamStr2 = getDeriverConfig();
            step2.setConfiguration(confParamStr2);

            // -----------
            List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
            steps.add(step1);
            steps.add(step2);

            // -----------
            configuration.setSteps(steps);

            return configuration;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String getMapperConfig(List<String> templates) throws JsonProcessingException {
        MapAttributeConfig config = new MapAttributeConfig();

        config.setStage(MapAttributeFlow.MAP_STAGE);
        config.setSource(targetSourceName);
        config.setTemplates(templates);
        config.setSeed(templates.get(0));
        config.setSeedId(keys[0][0]);

        config.setJoinConfigs(createJoinConfigs());

        return om.writeValueAsString(config);
    }

    private List<MapAttributeConfig.JoinConfig> createJoinConfigs() {
        List<MapAttributeConfig.JoinConfig> joinConfigs = new ArrayList<MapAttributeConfig.JoinConfig>();
        for (int i = 1; i < sources.length; i++) {
            MapAttributeConfig.JoinConfig joinConfig = new MapAttributeConfig.JoinConfig();
            joinConfig.setKeys(Arrays.asList(keys[i]));
            List<MapAttributeConfig.JoinTarget> joinTargets = new ArrayList<MapAttributeConfig.JoinTarget>();
            GeneralSource[] curSources = sources[i];
            for (int j = 0; j < curSources.length; j++) {
                MapAttributeConfig.JoinTarget joinTarget = new MapAttributeConfig.JoinTarget();
                joinTarget.setSource(curSources[j].getSourceName());
                joinTarget.setKeys(Arrays.asList(keys[i]));
                joinTargets.add(joinTarget);
                log.info("XXXX Join target " + curSources[j].getSourceName());
                for (int k = 0; k < keys[i].length; k++) {
                    log.info("XXXX Join Key " + keys[i][k]);
                }
            }
            joinConfig.setTargets(joinTargets);
            joinConfigs.add(joinConfig);
        }
        return joinConfigs;
    }

    private String getDeriverConfig() throws JsonProcessingException {

        DeriveAttributeConfig config = new DeriveAttributeConfig();

        config.setStage(MapAttributeFlow.DERIVE_STAGE);
        config.setSource(targetSourceName);
        List<String> templates = new ArrayList<String>();
        templates.add(targetSourceName);
        config.setTemplates(templates);

        return om.writeValueAsString(config);
    }

    @Override
    protected TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    protected Source getSource() {
        return new GeneralSource("Amtest");
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
        log.info("Start to verify records one by one.");

        int rowNum = 0;
        while (records.hasNext()) {
            String value = null;
            GenericRecord record = records.next();
            for (int i = 0; i < keys[0].length; i++) {
                String keyValue = record.get(keys[0][i]).toString();
                if (value == null) {
                    value = keyValue;
                } else {
                    Assert.assertEquals(keyValue, value);
                }
            }
            Long lValue= Long.valueOf(value);
            for (int i = 0; i < totalAttributes; i++) {
                Long attrVaule = (Long)record.get(ATTRIBUTE + i);
                Assert.assertEquals(attrVaule, lValue);
            }

            Long derivedValue = (Long)record.get(DERIVED_ATTRIBUTE);
            Assert.assertEquals(derivedValue, new Long(totalAttributes * lValue));
            rowNum++;
        }
        Assert.assertEquals(rowNum, numRecords);
    }
}
