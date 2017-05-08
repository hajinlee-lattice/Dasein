package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.BitCodecUtils;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.BomboraSurgePivotedFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.service.SourceService;
import com.latticeengines.datacloud.etl.transformation.service.TransformationService;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BomboraSurgeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class BomboraSurgePivotedServiceImplTestNG
        extends TransformationServiceImplTestNGBase<PipelineTransformationConfiguration> {
    private static final Log log = LogFactory.getLog(BomboraSurgePivotedServiceImplTestNG.class);

    GeneralSource source = new GeneralSource("BomboraSurgePivoted");
    GeneralSource bomboraSurge = new GeneralSource("BomboraSurge");

    @Autowired
    SourceService sourceService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    private PipelineTransformationService pipelineTransformationService;

    String targetSourceName = "BomboraSurgePivoted";

    ObjectMapper om = new ObjectMapper();

    @Autowired
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    private String[] topics = { "2-in-1 PCs", "3D Printing", "401k" };
    // ColumnName -> <BitPosition, BitUnit>
    private Map<String, Pair<Integer, Integer>> bmbrCompoScores = new HashMap<>();
    private Map<String, Pair<Integer, Integer>> bmbrBucketCodes = new HashMap<>();
    private Map<String, Pair<Integer, Integer>> bmbrIntents = new HashMap<>();

    @Test(groups = "functional")
    public void testTransformation() {
        prepareBomboraSurge();
        prepareEnDecodeHelper();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    private void prepareEnDecodeHelper() {
        for (int i = 0; i < topics.length; i++) {
            bmbrCompoScores.put("BmbrSurge_" + topics[i].replaceAll(" ", "").replace("-", "") + "_CompScore",
                    Pair.of(-1, -1));
            bmbrBucketCodes.put("BmbrSurge_" + topics[i].replaceAll(" ", "").replace("-", "") + "_BuckScore",
                    Pair.of(-1, -1));
            bmbrIntents.put("BmbrSurge_" + topics[i].replaceAll(" ", "").replace("-", "") + "_Intent", Pair.of(-1, -1));
        }
        List<SourceColumn> columns = sourceColumnEntityMgr.getSourceColumns(source.getSourceName());
        int n = 0;
        for (SourceColumn column : columns) {
            if (bmbrCompoScores.containsKey(column.getColumnName())) {
                bmbrCompoScores.put(column.getColumnName(), parseBitPos(column.getArguments()));
                n++;
            } else if (bmbrBucketCodes.containsKey(column.getColumnName())) {
                bmbrBucketCodes.put(column.getColumnName(), parseBitPos(column.getArguments()));
                n++;
            } else if (bmbrIntents.containsKey(column.getColumnName())) {
                bmbrIntents.put(column.getColumnName(), parseBitPos(column.getArguments()));
                n++;
            }
            if (n == 9) {
                break;
            }
        }
    }

    private Pair<Integer, Integer> parseBitPos(String arguments) {
        try {
            JsonNode jsonNode = om.readTree(arguments);
            return Pair.of(jsonNode.get("BitPosition").asInt(), jsonNode.get("BitUnit").asInt());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    PipelineTransformationConfiguration createTransformationConfiguration() {
        try {
            PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
            configuration.setName("BomboraSurgePivoted");
            configuration.setVersion(targetVersion);

            TransformationStepConfig step1 = new TransformationStepConfig();
            List<String> baseSources = new ArrayList<String>();
            baseSources.add(bomboraSurge.getSourceName());
            step1.setBaseSources(baseSources);
            step1.setTransformer(BomboraSurgePivotedFlow.TRANSFORMER_NAME);
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
        config.setBucketCodeField("BucketCode");
        config.setCompoScoreField("CompositeScore");
        return om.writeValueAsString(config);
    }

    private Object[][] data = new Object[][] { //
            { 1, "google.com", topics[0], 75, "A" }, //
            { 2, "google.com", topics[1], 55, "B" }, //
            { 3, "yahoo.com", topics[0], 45, "B" }, //
            { 4, "yahoo.com", topics[2], 25, "A" }, //
            { 5, "apple.com", "Dummy Topic", 15, "C" },
    };

    private void prepareBomboraSurge() {
        List<Pair<String, Class<?>>> columns = new ArrayList<>();
        columns.add(Pair.of("ID", Integer.class));
        columns.add(Pair.of("Domain", String.class));
        columns.add(Pair.of("Topic", String.class));
        columns.add(Pair.of("CompositeScore", Integer.class));
        columns.add(Pair.of("BucketCode", String.class));
        uploadBaseSourceData(bomboraSurge.getSourceName(), baseSourceVersion, columns, data);
    }

    @Override
    TransformationService<PipelineTransformationConfiguration> getTransformationService() {
        return pipelineTransformationService;
    }

    @Override
    Source getSource() {
        return source;
    }

    @Override
    String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    String getPathForResult() {
        Source targetSource = sourceService.findBySourceName(targetSourceName);
        String targetVersion = hdfsSourceEntityMgr.getCurrentVersion(targetSource);
        return hdfsPathBuilder.constructSnapshotDir(targetSourceName, targetVersion).toString();
    }

    @Override
    void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record);
            Object domain = record.get("Domain");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            Object encodedCompoScore = record.get("BmbrSurge_CompositeScore");
            if (encodedCompoScore instanceof Utf8) {
                encodedCompoScore = encodedCompoScore.toString();
            }
            if (StringUtils.isNotBlank((String) encodedCompoScore)) {
                try {
                    boolean[] bits = BitCodecUtils.decodeAll((String) encodedCompoScore);
                    log.info(Arrays.toString(bits));
                } catch (IOException e) {
                    log.error("Fail to decode encodedCompoScore: " + encodedCompoScore);
                }
            }
            Object encodedBucketCode = record.get("BmbrSurge_BucketCode");
            if (encodedBucketCode instanceof Utf8) {
                encodedBucketCode = encodedBucketCode.toString();
            }
            if (StringUtils.isNotBlank((String) encodedBucketCode)) {
                try {
                    boolean[] bits = BitCodecUtils.decodeAll((String) encodedBucketCode);
                    log.info(Arrays.toString(bits));
                } catch (IOException e) {
                    log.error("Fail to decode encodedBucketCode: " + encodedBucketCode);
                }
            }
            Object encodedIntent = record.get("BmbrSurge_Intent");
            if (encodedIntent instanceof Utf8) {
                encodedIntent = encodedIntent.toString();
            }
            if (StringUtils.isNotBlank((String) encodedIntent)) {
                try {
                    boolean[] bits = BitCodecUtils.decodeAll((String) encodedIntent);
                    log.info(Arrays.toString(bits));
                } catch (IOException e) {
                    log.error("Fail to decode encodedIntent: " + encodedIntent);
                }
            }
            rowNum++;
        }
        Assert.assertEquals(rowNum, 3);
    }
}
