package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.HGDataTechIndicatorsFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TechIndicatorsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.transform.v2_0_25.common.JsonUtils;

public class HGDataTechIndicatorsTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(HGDataTechIndicatorsTestNG.class);

    private final String SEGMENT_INDICATORS = "SegmentTechIndicators";
    private final String SUPPLIER_INDICATORS = "SupplierTechIndicators";

    private final String TECH_SEG1 = "TechIndicator_1010Data";  // single search key
    private final String TECH_SEG2 = "TechIndicator_CommVault"; // multi search key
    private final String TECH_SEG3 = "TechIndicator_AutonomyConnectedBackup";   // deprecated
    private final String TECH_SEG4 = "TechIndicator_11"; // single search key

    private final String TECH_SUPP1 = "TechIndicator_247Customer";  // single search key
    private final String TECH_SUPP2 = "TechIndicator_3M";   // single search key
    private final String TECH_SUPP3 = "TechIndicator_24_7"; // Deprecated
    private final String TECH_SUPP4 = "TechIndicator_ABB"; // Deprecated

    private int HAS_SEG1_POS = -1;
    private int HAS_SEG2_POS = -1;
    private int HAS_SEG3_POS = -1;
    private int HAS_SEG4_POS = -1;
    private int HAS_SUPP1_POS = -1;
    private int HAS_SUPP2_POS = -1;
    private int HAS_SUPP3_POS = -1;
    private int HAS_SUPP4_POS = -1;

    private final ObjectMapper om = new ObjectMapper();

    GeneralSource source = new GeneralSource("HGDataTechIndicators");
    GeneralSource hgClean = new GeneralSource("HGDataClean");
    GeneralSource hgTechInd = new GeneralSource("HGDataTechIndicators");

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    @Test(groups = "functional")
    public void testTransformation() {
        readBitPositions();
        prepareHGClean();
        prepareHGTechInd();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
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
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(source.getSourceName());
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("HGDataTechIndicators");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(hgClean.getSourceName());
            baseSources.add(hgTechInd.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(HGDataTechIndicatorsFlow.TRANSFORMER_NAME);
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
        String[] groupByFields = { "Domain" };
        config.setGroupByFields(groupByFields);
        config.setTimestampField("Timestamp");
        return om.writeValueAsString(config);
    }

    private Object[][] hgCleanData = new Object[][] { //
            { "google.com", "Supplier", "1010data" }, //
            { "google.com", "Supplier", "CommVault" }, //
            { "google.com", "24/7 Customer, Inc.", "Segment" }, //
            { "google.com", "3M Company", "Segment" }, //
            { "facebook.com", "Supplier", "1010data" }, //
            { "facebook.com", "Supplier", "CommVault" }, //
            { "facebook.com", "24/7 Customer, Inc.", "Segment" }, //
            { "facebook.com", "3M Company", "Segment" }, //
    };

    private void prepareHGClean() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("Supplier_Name", String.class));
        columns.add(Pair.of("Segment_Name", String.class));
        uploadBaseSourceData(hgClean.getSourceName(), baseSourceVersion, columns, hgCleanData);
    }

    private Object[][] preparePreviousHGTechIndData() {
        Object[][] data = new Object[1][3];
        data[0][0] = "google.com";
        List<SourceColumn> srcCols = sourceColumnEntityMgr.getSourceColumns(source.getSourceName());
        List<Integer> supplierTrueBits = new ArrayList<>();
        List<Integer> segmentTrueBits = new ArrayList<>();
        for (SourceColumn srcCol : srcCols) {
            if (srcCol.getColumnName().equals(TECH_SEG3)) {
                JsonNode args = JsonUtils.deserialize(srcCol.getArguments(), JsonNode.class);
                segmentTrueBits.add(args.get("DecodeStrategy").get("BitPosition").asInt());
            }
            if (srcCol.getColumnName().equals(TECH_SUPP3)) {
                JsonNode args = JsonUtils.deserialize(srcCol.getArguments(), JsonNode.class);
                supplierTrueBits.add(args.get("DecodeStrategy").get("BitPosition").asInt());
            }
        }
        try {
            data[0][1] = BitCodecUtils.encode(supplierTrueBits.stream().mapToInt(i -> i).toArray());
            data[0][2] = BitCodecUtils.encode(segmentTrueBits.stream().mapToInt(i -> i).toArray());
        } catch (IOException e) {
            throw new RuntimeException("Fail to encode", e);
        }
        return data;
    }

    private void prepareHGTechInd() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("SupplierTechIndicators", String.class));
        columns.add(Pair.of("SegmentTechIndicators", String.class));
        uploadBaseSourceData(hgTechInd.getSourceName(), baseSourceVersion, columns, preparePreviousHGTechIndData());
    }

    @Override
    public void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int recordsToCheck = 100;
        int pos = 0;
        log.info("HAS_SEG1_POS: " + HAS_SEG1_POS);
        log.info("HAS_SEG2_POS: " + HAS_SEG2_POS);
        log.info("HAS_SEG3_POS: " + HAS_SEG3_POS);
        log.info("HAS_SEG4_POS: " + HAS_SEG4_POS);
        log.info("HAS_SUPP1_POS: " + HAS_SUPP1_POS);
        log.info("HAS_SUPP2_POS: " + HAS_SUPP2_POS);
        log.info("HAS_SUPP3_POS: " + HAS_SUPP3_POS);
        log.info("HAS_SUPP4_POS: " + HAS_SUPP4_POS);
        while (pos++ < recordsToCheck && records.hasNext()) {
            GenericRecord record = records.next();
            String domain = record.get("Domain").toString();
            try {
                boolean[] bits = BitCodecUtils.decode(record.get(SEGMENT_INDICATORS).toString(),
                        new int[] { HAS_SEG1_POS, HAS_SEG2_POS, HAS_SEG3_POS, HAS_SEG4_POS });
                boolean[] bits2 = BitCodecUtils.decode(record.get(SUPPLIER_INDICATORS).toString(),
                        new int[] { HAS_SUPP1_POS, HAS_SUPP2_POS, HAS_SUPP3_POS, HAS_SUPP4_POS });
                if ("google.com".equals(domain)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertTrue(bits2[0]);
                    Assert.assertTrue(bits[1]);
                    Assert.assertTrue(bits2[1]);
                    Assert.assertTrue(bits[2]);
                    Assert.assertTrue(bits2[2]);
                    Assert.assertFalse(bits[3]);
                    Assert.assertFalse(bits2[3]);
                }
                if ("facebook.com".equals(domain)) {
                    Assert.assertTrue(bits[0]);
                    Assert.assertTrue(bits2[0]);
                    Assert.assertTrue(bits[1]);
                    Assert.assertTrue(bits2[1]);
                    Assert.assertFalse(bits[2]);
                    Assert.assertFalse(bits2[2]);
                    Assert.assertFalse(bits[3]);
                    Assert.assertFalse(bits2[3]);
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
            if (TECH_SEG1.equals(columnName)) {
                HAS_SEG1_POS = parseBitPos(column.getArguments());
            } else if (TECH_SEG2.equals(columnName)) {
                HAS_SEG2_POS = parseBitPos(column.getArguments());
            } else if (TECH_SEG3.equals(columnName)) {
                HAS_SEG3_POS = parseBitPos(column.getArguments());
            } else if (TECH_SEG4.equals(columnName)) {
                HAS_SEG4_POS = parseBitPos(column.getArguments());
            } else if (TECH_SUPP1.equals(columnName)) {
                HAS_SUPP1_POS = parseBitPos(column.getArguments());
            } else if (TECH_SUPP2.equals(columnName)) {
                HAS_SUPP2_POS = parseBitPos(column.getArguments());
            } else if (TECH_SUPP3.equals(columnName)) {
                HAS_SUPP3_POS = parseBitPos(column.getArguments());
            } else if (TECH_SUPP4.equals(columnName)) {
                HAS_SUPP4_POS = parseBitPos(column.getArguments());
            }
            if (Collections
                    .min(Arrays.asList(HAS_SEG1_POS, HAS_SEG2_POS, HAS_SEG3_POS, HAS_SEG4_POS, HAS_SUPP1_POS,
                            HAS_SUPP2_POS, HAS_SUPP3_POS, HAS_SUPP4_POS)) > -1) {
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
