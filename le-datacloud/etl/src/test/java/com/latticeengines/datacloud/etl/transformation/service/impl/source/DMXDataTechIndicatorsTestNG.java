package com.latticeengines.datacloud.etl.transformation.service.impl.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.source.DMXDataTechIndicatorsFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.TechIndicatorsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class DMXDataTechIndicatorsTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(DMXDataTechIndicatorsTestNG.class);
    private final String PRODUCT_INDICATORS = "DMXProductTechIndicators";
    private final String VENDOR_INDICATORS = "DMXVendorTechIndicators";
    private final String SOLUTION_INDICATORS = "DMXSolutionTechIndicators";
    
    private final String TECH_PRD1 = "DMX_Prd_ACCELOPS";
    private final String TECH_PRD2 = "DMX_Prd_ACONEX";

    private final String TECH_VEN1 = "DMX_Ven_bea.com";
    private final String TECH_VEN2 = "DMX_Ven_bell.ca";
    
    private final String TECH_SOL1 = "DMX_Sol_SECURITY solution";
    private final String TECH_SOL2 = "DMX_Sol_TIME TRACKING SOFTWARE solution";
    
    private int HAS_PRD1_POS = -1;
    private int HAS_PRD2_POS = -1;
    private int HAS_VEN1_POS = -1;
    private int HAS_VEN2_POS = -1;
    private int HAS_SOL1_POS = -1;
    private int HAS_SOL2_POS = -1;
    
    private final ObjectMapper om = new ObjectMapper();

    private GeneralSource source = new GeneralSource("DMXDataTechIndicators");
    private GeneralSource dmxClean = new GeneralSource("DMXDataClean");

    @Inject
    private SourceColumnEntityMgr sourceColumnEntityMgr;
    
    @Test(groups = "pipeline2", enabled = true)
    public void testTransformation() {
        readBitPositions();
        prepareDmxClean();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }
    
    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("DMXDataTechIndicators");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<>();
            baseSources.add(dmxClean.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(DMXDataTechIndicatorsFlow.TRANSFORMER_NAME);
            step1.setTargetSource(source.getSourceName());
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
        TechIndicatorsConfig config = new TechIndicatorsConfig();
        String[] groupByFields = { "Duns" };
        config.setGroupByFields(groupByFields);
        return om.writeValueAsString(config);
    }
    
    private Object[][] dmxCleanData = new Object[][] { //
        { "060902413", "Supplier", "ACCELOPS", "ACCELOPS", "High" }, //
        { "060902413", "Supplier", "ACONEX", "ACONEX", "Medium" }, //
        { "060902413", "bea.com", "Segment", "bea.com", "Low" }, //
        { "060902413", "bell.ca", "Segment", "bell.ca", "High" }, //
        { "060902413", "SECURITY solution", "SECURITY solution", "Collection", "Medium" }, //
        { "060902413", "TIME TRACKING SOFTWARE solution", "TIME TRACKING SOFTWARE solution", "Collection", "Low" }, //
        { "196337864", "Supplier", "ACCELOPS", "ACCELOPS", "High" }, //
        { "196337864", "Supplier", "ACONEX", "ACONEX", "Medium" }, //
        { "196337864", "bea.com", "Segment", "bea.com", "Low" }, //
        { "196337864", "bell.ca", "Segment", "bell.ca", "Low" }, //
        { "196337864", "SECURITY solution", "SECURITY solution", "Collection", "Medium" }, //
        { "196337864", "TIME TRACKING SOFTWARE solution", "TIME TRACKING SOFTWARE solution", "Collection", "High" }, //
        { "072148831", "Supplier", "ACCELOPS", "ACCELOPS", "Medium" }, //
        { "072148831", "Supplier", "ACONEX", "ACONEX", "High" }, //
        { "072148831", "bea.com", "Segment", "bea.com", "Low" }, //
        { "072148831", "bell.ca", "Segment", "bell.ca", "Medium" }, //
        { "072148831", "SECURITY solution", "SECURITY solution", "Collection", "High" }, //
        { "072148831", "TIME TRACKING SOFTWARE solution", "TIME TRACKING SOFTWARE solution", "Collection", "Low" }, //
    };

    private void prepareDmxClean() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Duns", String.class));
        columns.add(Pair.of("Supplier_Name", String.class));
        columns.add(Pair.of("Segment_Name", String.class));
        columns.add(Pair.of("Collection_Name", String.class));
        columns.add(Pair.of("intensity", String.class));
        uploadBaseSourceData(dmxClean.getSourceName(), baseSourceVersion, columns, dmxCleanData);
    }
    
    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int recordsToCheck = 100;
        int pos = 0;
        log.info("HAS_PRD1_POS: " + HAS_PRD1_POS);
        log.info("HAS_PRD2_POS: " + HAS_PRD2_POS);
        log.info("HAS_VEN1_POS: " + HAS_VEN1_POS);
        log.info("HAS_VEN2_POS: " + HAS_VEN2_POS);
        log.info("HAS_SOL1_POS: " + HAS_SOL1_POS);
        log.info("HAS_SOL2_POS: " + HAS_SOL2_POS);
        while (pos++ < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            String duns = record.get("Duns").toString();
            try {
                boolean[] bits = BitCodecUtils.decode(record.get(PRODUCT_INDICATORS).toString(),
                        new int[] { HAS_PRD1_POS, HAS_PRD2_POS });
                boolean[] bits2 = BitCodecUtils.decode(record.get(VENDOR_INDICATORS).toString(),
                        new int[] { HAS_VEN1_POS, HAS_VEN2_POS });
                boolean[] bits3 = BitCodecUtils.decode(record.get(SOLUTION_INDICATORS).toString(),
                        new int[] { HAS_SOL1_POS, HAS_SOL2_POS });
                if ("060902413".equals(duns)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertFalse(bits2[0]);
                    Assert.assertFalse(bits3[0]);
                    Assert.assertFalse(bits[1]);
                    Assert.assertFalse(bits2[1]);
                    Assert.assertFalse(bits3[1]);
                }
                if ("196337864".equals(duns)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertFalse(bits2[0]);
                    Assert.assertFalse(bits3[0]);
                    Assert.assertFalse(bits[1]);
                    Assert.assertFalse(bits2[1]);
                    Assert.assertFalse(bits3[1]);
                }
                if ("072148831".equals(duns)) {
                    Assert.assertFalse(bits[0]);
                    Assert.assertFalse(bits2[0]);
                    Assert.assertFalse(bits3[0]);
                    Assert.assertTrue(bits[1]);
                    Assert.assertFalse(bits2[1]);
                    Assert.assertFalse(bits3[1]);
                }
            } catch (IOException e) {
                System.out.println(record);
                throw new RuntimeException(e);
            }
        }
    }
    
    private void readBitPositions() {
        List<SourceColumn> columns = sourceColumnEntityMgr.getSourceColumns(source.getSourceName());
        for (SourceColumn column : columns) {
            String columnName = column.getColumnName();
            if (TECH_PRD1.equals(columnName)) {
                HAS_PRD1_POS = parseBitPos(column.getArguments());
            } else if (TECH_PRD2.equals(columnName)) {
                HAS_PRD2_POS = parseBitPos(column.getArguments());
            } else if (TECH_VEN1.equals(columnName)) {
                HAS_VEN1_POS = parseBitPos(column.getArguments());
            } else if (TECH_VEN2.equals(columnName)) {
                HAS_VEN2_POS = parseBitPos(column.getArguments());
            } else if (TECH_SOL1.equals(columnName)) {
                HAS_SOL1_POS = parseBitPos(column.getArguments());
            } else if (TECH_SOL2.equals(columnName)) {
                HAS_SOL2_POS = parseBitPos(column.getArguments());
            }
            if (Collections
                    .min(Arrays.asList(HAS_PRD1_POS, HAS_PRD2_POS, HAS_VEN1_POS, HAS_VEN2_POS, HAS_SOL1_POS,
                            HAS_SOL2_POS)) > -1) {
                break;
            }
        }
    }

    private int parseBitPos(String arguments) {
        try {
            JsonNode jsonNode = om.readTree(arguments);
            return jsonNode.get("BitPosition").asInt();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
