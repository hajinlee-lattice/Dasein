package com.latticeengines.datacloud.etl.transformation.service.impl.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.source.BomboraSurgePivotedFlow;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.BomboraSurgeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;
import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook.DecodeStrategy;

public class BomboraSurgePivotedTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(BomboraSurgePivotedTestNG.class);

    private GeneralSource source = new GeneralSource("BomboraSurgePivoted");
    private GeneralSource bomboraSurge = new GeneralSource("BomboraSurge");

    private ObjectMapper om = new ObjectMapper();

    @Inject
    private SourceColumnEntityMgr sourceColumnEntityMgr;

    private String[] topics = { "2-in-1 PCs", "3D Printing", "401k" };
    // ColumnName -> <BitPosition, BitUnit>
    private Map<String, Pair<Integer, Integer>> bmbrCompoScores = new HashMap<>();
    private Map<String, Pair<Integer, Integer>> bmbrBucketCodes = new HashMap<>();
    private Map<String, Pair<Integer, Integer>> bmbrIntents = new HashMap<>();

    private BitCodeBook compoScoreCodeBook;
    private BitCodeBook bucketScoreCodeBook;
    private BitCodeBook intentCodeBook;
    private List<String> compoScoreDecodeFields;
    private List<String> bucketScoreDecodeFields;
    private List<String> intentDecodeFields;

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

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
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
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("BomboraSurgePivoted");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(bomboraSurge.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(BomboraSurgePivotedFlow.TRANSFORMER_NAME);
        step1.setTargetSource(source.getSourceName());
        String confParamStr1 = getTransformerConfig();
        step1.setConfiguration(confParamStr1);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);

        return configuration;
    }

    private String getTransformerConfig() {
        BomboraSurgeConfig config = new BomboraSurgeConfig();
        config.setBucketCodeField("BucketCode");
        config.setCompoScoreField("CompositeScore");
        return JsonUtils.serialize(config);
    }

    private Object[][] data = new Object[][] { //
            { 1, "google.com", topics[0], 75, "A" }, // High
            { 2, "google.com", topics[1], 60, "B" }, // Moderate
            { 3, "yahoo.com", topics[0], 67, "B" }, // Moderate
            { 4, "yahoo.com", topics[2], 25, "A" }, // Normal
            { 5, "apple.com", "Dummy Topic", 15, "C" }, // Normal
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

    private Object[][] expected = { //
            { "yahoo.com", 67, null, 25, "B", null, "A", "Moderate", null, "Normal" }, //
            { "google.com", 75, 60, null, "A", "B", null, "High", "Moderate", null }
    };

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
        prepareBitCodeBook();
        Map<String, Map<String, Object>> expectedMap = new HashMap<>();
        for (Object[] data : expected) {
            Map<String, Object> map = new HashMap<>();
            map.put("BmbrSurge_2in1PCs_CompScore", data[1]);
            map.put("BmbrSurge_3DPrinting_CompScore", data[2]);
            map.put("BmbrSurge_401k_CompScore", data[3]);
            map.put("BmbrSurge_2in1PCs_BuckScore", data[4]);
            map.put("BmbrSurge_3DPrinting_BuckScore", data[5]);
            map.put("BmbrSurge_401k_BuckScore", data[6]);
            map.put("BmbrSurge_2in1PCs_Intent", data[7]);
            map.put("BmbrSurge_3DPrinting_Intent", data[8]);
            map.put("BmbrSurge_401k_Intent", data[9]);
            expectedMap.put((String) data[0], map);
        }
        log.info("Start to verify records one by one.");
        int rowNum = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            Object domain = record.get("Domain");
            if (domain instanceof Utf8) {
                domain = domain.toString();
            }
            Object encodedCompoScore = record.get("BmbrSurge_CompositeScore");
            if (encodedCompoScore instanceof Utf8) {
                encodedCompoScore = encodedCompoScore.toString();
            }
            if (StringUtils.isNotBlank((String) encodedCompoScore)) {
                Map<String, Object> compoScores = compoScoreCodeBook.decode((String) encodedCompoScore,
                        compoScoreDecodeFields);
                for (Map.Entry<String, Object> entry : compoScores.entrySet()) {
                    log.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
                    Assert.assertTrue(isObjEquals(entry.getValue(), expectedMap.get(domain).get(entry.getKey())));
                }
            }
            Object encodedBucketCode = record.get("BmbrSurge_BucketCode");
            if (encodedBucketCode instanceof Utf8) {
                encodedBucketCode = encodedBucketCode.toString();
            }
            if (StringUtils.isNotBlank((String) encodedBucketCode)) {
                Map<String, Object> bucketScores = bucketScoreCodeBook.decode((String) encodedBucketCode,
                        bucketScoreDecodeFields);
                for (Map.Entry<String, Object> entry : bucketScores.entrySet()) {
                    log.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
                    Assert.assertTrue(isObjEquals(entry.getValue(), expectedMap.get(domain).get(entry.getKey())));
                }
            }
            Object encodedIntent = record.get("BmbrSurge_Intent");
            if (encodedIntent instanceof Utf8) {
                encodedIntent = encodedIntent.toString();
            }
            if (StringUtils.isNotBlank((String) encodedIntent)) {
                Map<String, Object> intents = intentCodeBook.decode((String) encodedIntent, intentDecodeFields);
                for (Map.Entry<String, Object> entry : intents.entrySet()) {
                    log.info(String.format("%s: %s", entry.getKey(), entry.getValue()));
                    Assert.assertTrue(isObjEquals(entry.getValue(), expectedMap.get(domain).get(entry.getKey())));
                }
            }
            rowNum++;
        }
        Assert.assertEquals(rowNum, 3);
    }

    private void prepareBitCodeBook() {
        List<SourceColumn> srcCols = sourceColumnEntityMgr.getSourceColumns(source.getSourceName());
        Map<String, Integer> bitsPosMap = srcCols.stream()
                .filter(srcCol -> SourceColumn.Calculation.BIT_ENCODE.equals(srcCol.getCalculation()))
                .collect(Collectors.toMap(srcCol -> srcCol.getColumnName(),
                        srcCol -> {
                            try {
                                return om.readTree(srcCol.getArguments()).get("BitPosition").asInt();
                            } catch (IOException e) {
                                throw new RuntimeException("Fail to parse " + srcCol.getArguments());
                            }
                        }));

        compoScoreCodeBook = new BitCodeBook();
        compoScoreDecodeFields = new ArrayList<>();
        compoScoreCodeBook.setDecodeStrategy(DecodeStrategy.NUMERIC_UNSIGNED_INT);
        compoScoreCodeBook.setBitsPosMap(bitsPosMap);
        compoScoreDecodeFields.add("BmbrSurge_2in1PCs_CompScore");
        compoScoreDecodeFields.add("BmbrSurge_3DPrinting_CompScore");
        compoScoreDecodeFields.add("BmbrSurge_401k_CompScore");
        compoScoreCodeBook.setBitUnit(8);

        bucketScoreCodeBook = new BitCodeBook();
        bucketScoreDecodeFields = new ArrayList<>();
        bucketScoreCodeBook.setDecodeStrategy(DecodeStrategy.ENUM_STRING);
        bucketScoreCodeBook.setBitsPosMap(bitsPosMap);
        bucketScoreDecodeFields.add("BmbrSurge_2in1PCs_BuckScore");
        bucketScoreDecodeFields.add("BmbrSurge_3DPrinting_BuckScore");
        bucketScoreDecodeFields.add("BmbrSurge_401k_BuckScore");
        bucketScoreCodeBook.setBitUnit(2);
        Map<String, Object> valueDictRev = new HashMap<>();
        valueDictRev.put("1", "A");
        valueDictRev.put("10", "B");
        valueDictRev.put("11", "C");
        bucketScoreCodeBook.setValueDictRev(valueDictRev);

        intentCodeBook = new BitCodeBook();
        intentDecodeFields = new ArrayList<>();
        intentCodeBook.setDecodeStrategy(DecodeStrategy.ENUM_STRING);
        intentCodeBook.setBitsPosMap(bitsPosMap);
        intentDecodeFields.add("BmbrSurge_2in1PCs_Intent");
        intentDecodeFields.add("BmbrSurge_3DPrinting_Intent");
        intentDecodeFields.add("BmbrSurge_401k_Intent");
        intentCodeBook.setBitUnit(3);
        valueDictRev = new HashMap<>();
        valueDictRev.put("1", "Normal");
        valueDictRev.put("10", "Moderate");
        valueDictRev.put("11", "High");
        intentCodeBook.setValueDictRev(valueDictRev);
    }
}
