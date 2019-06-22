package com.latticeengines.scoringapi.score.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugRecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.scoringapi.controller.ScoringResourceDeploymentTestNG;
import com.latticeengines.scoringapi.controller.TestModelArtifactDataComposition;
import com.latticeengines.scoringapi.controller.TestModelConfiguration;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.match.Matcher;
import com.latticeengines.scoringapi.score.AdditionalScoreConfig;
import com.latticeengines.scoringapi.score.BulkMatchingContext;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

public class ScoreRequestProcessorDeploymentTestNG extends ScoringResourceDeploymentTestNG {
    private static final Logger log = LoggerFactory.getLogger(ScoreRequestProcessorDeploymentTestNG.class);

    private static final int MAX_RECORD_COUNT = 19; // prime number for better
                                                    // distribution of models
    private ScoreRequestProcessor scoreRequestProcessor;
    private ScoreRequestProcessorImpl scoreRequestProcessorImpl;
    private List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList;
    private BulkRecordScoreRequest request;
    private Map<RecordModelTuple, Map<String, Object>> unorderedMatchedRecordMap;
    @SuppressWarnings("unused")
    private Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap;
    private Map<String, Map<String, Predefined>> recordModelIdSelectionMap;
    private AdditionalScoreConfig additionalScoreConfig;
    private BulkMatchingContext bulkMatchingConfig;

    public void init() throws IOException {
        scoreRequestProcessor = applicationContext.getBean("customScoreRequestProcessor", ScoreRequestProcessor.class);
        scoreRequestProcessorImpl = (ScoreRequestProcessorImpl) scoreRequestProcessor;
        modelList = createModelList();

        additionalScoreConfig = AdditionalScoreConfig.instance() //
                .setSpace(customerSpace);

        bulkMatchingConfig = BulkMatchingContext.instance();
    }

    private void overwritePredifinedSelection() {
        int i = 0;
        for (String modelId : bulkMatchingConfig.getUniqueScoringArtifactsMap().keySet()) {
            ModelSummary modelSummary = bulkMatchingConfig.getUniqueScoringArtifactsMap().get(modelId).getValue()
                    .getModelSummary();
            Predefined predefinedSelection = (i++ % 2 == 0 ? Predefined.Model : Predefined.RTS);
            modelSummary.setPredefinedSelection(predefinedSelection);
            System.out.println(modelSummary.getId() + " -- " + predefinedSelection);
        }

        recordModelIdSelectionMap = new HashMap<>();

        for (Record record : request.getRecords()) {
            Map<String, Predefined> modelIdSelectionMap = new HashMap<>();

            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                modelIdSelectionMap.put(modelId, bulkMatchingConfig.getUniqueScoringArtifactsMap().get(modelId)
                        .getValue().getModelSummary().getPredefinedSelection());
            }

            recordModelIdSelectionMap.put(record.getRecordId(), modelIdSelectionMap);
        }
    }

    @Test(groups = "deployment", enabled = true)
    public void testFetchModelArtifacts() throws IOException {
        init();
        request = getBulkScoreRequest(MAX_RECORD_COUNT, modelList, false);
        List<Record> records = request.getRecords();

        scoreRequestProcessorImpl.fetchModelArtifacts(additionalScoreConfig, records, bulkMatchingConfig);
        Assert.assertEquals(MAX_RECORD_COUNT, records.size());
        Assert.assertEquals(modelList.size(), bulkMatchingConfig.getUniqueScoringArtifactsMap().size());
        Assert.assertEquals(modelList.size(), bulkMatchingConfig.getUniqueFieldSchemasMap().size());
        overwritePredifinedSelection();
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFetchModelArtifacts" })
    public void testCheckForDUNSField() throws IOException {
        BulkRecordScoreRequest bulkRequest = new BulkRecordScoreRequest();
        bulkRequest.setSource("Dummy Source");
        List<Record> records = request.getRecords();
        if (records != null && records.size() > 0) {
            Record record = records.get(records.size() - 1);
            List<Record> dunsRecords = new ArrayList<>();
            Record dunsRecord = cloneRecord(record);
            Record missingDUNSRecord = cloneRecord(record);
            for (Entry<String, Map<String, Object>> entry : dunsRecord.getModelAttributeValuesMap().entrySet()) {
                entry.getValue().remove(MISSING_FIELD_COMPANYNAME);
                entry.getValue().remove(MISSING_FIELD_EMAIL);
                entry.getValue().remove(MISSING_FIELD_WEBSITE);
            }
            for (Entry<String, Map<String, Object>> entry : missingDUNSRecord.getModelAttributeValuesMap().entrySet()) {
                entry.getValue().remove(MISSING_FIELD_DUNS);
            }
            dunsRecords.add(dunsRecord);
            dunsRecords.add(missingDUNSRecord);
            bulkRequest.setRecords(dunsRecords);

            AdditionalScoreConfig additionalScoreConfig = AdditionalScoreConfig.instance() //
                    .setSpace(customerSpace);

            bulkMatchingConfig.setOriginalOrderParsedTupleList(scoreRequestProcessorImpl
                    .checkForMissingFields(bulkMatchingConfig, bulkRequest, additionalScoreConfig));

            for (RecordModelTuple tuple : bulkMatchingConfig.getOriginalOrderParsedTupleList()) {
                Assert.assertNull(tuple.getException());
            }
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testFetchModelArtifacts" })
    public void testCheckForMissingFields() throws IOException {
        bulkMatchingConfig.setOriginalOrderParsedTupleList(
                scoreRequestProcessorImpl.checkForMissingFields(bulkMatchingConfig, request, additionalScoreConfig));
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY,
                bulkMatchingConfig.getOriginalOrderParsedTupleList().size());

        int idx = 0;
        for (RecordModelTuple tuple : bulkMatchingConfig.getOriginalOrderParsedTupleList()) {
            Assert.assertNull(tuple.getException());
            Record record = request.getRecords().get(idx / RECORD_MODEL_CARDINALITY);
            Assert.assertEquals(record, tuple.getRecord());

            Assert.assertEquals(RECORD_MODEL_CARDINALITY, record.getModelAttributeValuesMap().size());

            boolean foundModelIdMatch = false;
            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                if (modelId.equals(tuple.getModelId())) {
                    foundModelIdMatch = true;
                }
            }

            Assert.assertTrue(foundModelIdMatch);
            idx++;
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testCheckForMissingFields" })
    public void testExtractParsedList() {
        scoreRequestProcessorImpl.extractParsedList(additionalScoreConfig, bulkMatchingConfig);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testExtractParsedList" })
    public void testExtractModelSummaries() {
        bulkMatchingConfig
                .setOriginalOrderModelSummaryList(scoreRequestProcessorImpl.extractModelSummaries(bulkMatchingConfig));
        Assert.assertNotNull(bulkMatchingConfig.getOriginalOrderModelSummaryList());
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY,
                bulkMatchingConfig.getOriginalOrderModelSummaryList().size());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testExtractModelSummaries" })
    public void testBulkMatchAndJoin() {
        Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap = scoreRequestProcessorImpl
                .getMatcher(true).matchAndJoin(additionalScoreConfig, bulkMatchingConfig,
                        bulkMatchingConfig.getPartiallyOrderedParsedRecordWithMatchReqList(), false);

        unorderedMatchedRecordMap = extractMap(unorderedMatchedRecordEnrichmentMap, Matcher.RESULT);
        unorderedLeadEnrichmentMap = extractMap(unorderedMatchedRecordEnrichmentMap, Matcher.ENRICHMENT);
        Assert.assertNotNull(unorderedMatchedRecordMap);
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY, unorderedMatchedRecordMap.size());

        checkMatchedResults();
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testBulkMatchAndJoin" })
    public void testAddMissingFields() {
        scoreRequestProcessorImpl.addMissingFields(bulkMatchingConfig, unorderedMatchedRecordMap);
        bulkMatchingConfig.getUnorderedCombinedRecordMap().putAll(unorderedMatchedRecordMap);
        Assert.assertNotNull(bulkMatchingConfig.getUnorderedCombinedRecordMap());
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY,
                bulkMatchingConfig.getUnorderedCombinedRecordMap().size());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testAddMissingFields" })
    public void testTransform() {
        bulkMatchingConfig.setUnorderedTransformedRecords(scoreRequestProcessorImpl.transform(bulkMatchingConfig));

        Assert.assertNotNull(bulkMatchingConfig.getUnorderedTransformedRecords());
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY,
                bulkMatchingConfig.getUnorderedTransformedRecords().size());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testTransform" })
    public void testGenerateDebugScoreResponse() {
        boolean originalFlag = additionalScoreConfig.isDebug();
        additionalScoreConfig.setDebug(true);
        List<RecordScoreResponse> recordScoreResponseDebugList = scoreRequestProcessorImpl
                .generateDebugScoreResponse(additionalScoreConfig, bulkMatchingConfig);
        additionalScoreConfig.setDebug(originalFlag);

        Assert.assertNotNull(recordScoreResponseDebugList);
        Assert.assertEquals(MAX_RECORD_COUNT, recordScoreResponseDebugList.size());
        Assert.assertNotNull(recordScoreResponseDebugList.get(0).getScores().get(0).getScore());
        Assert.assertNotNull(recordScoreResponseDebugList.get(0).getScores().get(0).getProbability(),
                JsonUtils.serialize(recordScoreResponseDebugList.get(0).getScores().get(0)));
        Assert.assertTrue(recordScoreResponseDebugList.get(0).getScores().get(0).getProbability()
                .doubleValue() != recordScoreResponseDebugList.get(0).getScores().get(0).getScore().doubleValue());

        checkScoreResultList(recordScoreResponseDebugList, true);

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGenerateDebugScoreResponse" })
    public void testGenerateScoreResponse() {
        boolean originalFlag = additionalScoreConfig.isDebug();
        additionalScoreConfig.setDebug(false);
        List<RecordScoreResponse> recordScoreResponseList = scoreRequestProcessorImpl
                .generateScoreResponse(additionalScoreConfig, bulkMatchingConfig);
        additionalScoreConfig.setDebug(originalFlag);

        Assert.assertNotNull(recordScoreResponseList);
        Assert.assertEquals(MAX_RECORD_COUNT, recordScoreResponseList.size());
        Assert.assertNull(recordScoreResponseList.get(0).getScores().get(0).getProbability());
        Assert.assertNotNull(recordScoreResponseList.get(0).getScores().get(0).getScore());

        checkScoreResultList(recordScoreResponseList, false);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGenerateScoreResponse" })
    public void testBulkMatchAndJoinEnrichOnly() {
        Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap = scoreRequestProcessorImpl
                .getMatcher(true).matchAndJoin(additionalScoreConfig, bulkMatchingConfig,
                        bulkMatchingConfig.getPartiallyOrderedParsedRecordWithMatchReqList(), true);

        Map<RecordModelTuple, Map<String, Object>> matchedResult = extractMap(unorderedMatchedRecordEnrichmentMap,
                Matcher.RESULT);
        Assert.assertNotNull(matchedResult);
        Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap = bulkMatchingConfig
                .getUniqueScoringArtifactsMap();
        bulkMatchingConfig.setUniqueScoringArtifactsMap(null);
        if (0 != matchedResult.size()) {
            log.info(String.format("additionalScoreConfig = %s", JsonUtils.serialize(additionalScoreConfig)));
            log.info(String.format("bulkMatchingConfig = %s", JsonUtils.serialize(bulkMatchingConfig)));

        }
        Assert.assertEquals(matchedResult.size(), 0,
                String.format("matchedResult = %s", JsonUtils.serialize(matchedResult)));
        bulkMatchingConfig.setUniqueScoringArtifactsMap(uniqueScoringArtifactsMap);
        Map<RecordModelTuple, Map<String, Object>> enrichmentMap = extractMap(unorderedMatchedRecordEnrichmentMap,
                Matcher.ENRICHMENT);
        Assert.assertNotNull(enrichmentMap);
        Assert.assertEquals(enrichmentMap.size(), MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY,
                JsonUtils.serialize(enrichmentMap));
    }

    @Override
    protected boolean shouldRunScoringTest() {
        return false;
    }

    @Override
    protected boolean shouldSelectAttributeBeforeTest() {
        return true;
    }

    private void checkScoreResultList(List<RecordScoreResponse> recordScoreResponseList, boolean isDebug) {
        for (RecordScoreResponse recordScoreResponse : recordScoreResponseList) {
            if (isDebug) {
                DebugRecordScoreResponse debugResponse = (DebugRecordScoreResponse) recordScoreResponse;
                Assert.assertNotNull(debugResponse.getTransformedRecordMap());
                Assert.assertTrue(debugResponse.getTransformedRecordMap().size() > 0);
                Assert.assertNotNull(debugResponse.getMatchLogs());
                Assert.assertNotNull(debugResponse.getMatchErrorMessages());
                Assert.assertNotNull(debugResponse.getTransformedRecordMapTypes());
            }
            Assert.assertEquals(RECORD_MODEL_CARDINALITY, recordScoreResponse.getScores().size());
            for (ScoreModelTuple score : recordScoreResponse.getScores()) {
                Assert.assertNotNull(score);
                Assert.assertNotNull(score.getModelId());
                Assert.assertNotNull(score.getScore());
                System.out.println(score.getModelId() + " => " + score.getScore());
            }
        }
    }

    private void checkMatchedResults() {
        Map<String, List<RecordModelTuple>> tempGrouping = new HashMap<>();

        for (RecordModelTuple tuple : unorderedMatchedRecordMap.keySet()) {
            List<RecordModelTuple> tupleList = tempGrouping.get(tuple.getRecord().getRecordId());
            if (tupleList == null) {
                tupleList = new ArrayList<>();
                tempGrouping.put(tuple.getRecord().getRecordId(), tupleList);
            }

            tupleList.add(tuple);
        }

        for (String recordId : tempGrouping.keySet()) {
            Map<String, Predefined> modelIdSelectionMap = recordModelIdSelectionMap.get(recordId);
            List<RecordModelTuple> relatedRecordModelTuple = tempGrouping.get(recordId);
            Assert.assertEquals(RECORD_MODEL_CARDINALITY, relatedRecordModelTuple.size());
            for (RecordModelTuple tuple : relatedRecordModelTuple) {
                Map<String, Object> matchedResult = unorderedMatchedRecordMap.get(tuple);
                String modelId = tuple.getModelId();

                Predefined columnSelection = bulkMatchingConfig.getUniqueScoringArtifactsMap().get(modelId).getValue()
                        .getModelSummary().getPredefinedSelection();

                Assert.assertEquals(modelIdSelectionMap.get(modelId), columnSelection);
                if (columnSelection == Predefined.Model) {
                    Assert.assertTrue(matchedResult.containsKey("CloudTechnologies_CustomerOrderManagement"));
                    Assert.assertTrue(matchedResult.containsKey("AlexaGBUsers"));
                    Assert.assertTrue(matchedResult.containsKey("CloudTechnologies_WCMS"));
                    Assert.assertTrue(matchedResult.containsKey("AlexaAUUsers"));
                } else if (columnSelection == Predefined.RTS) {
                    Assert.assertTrue(matchedResult.containsKey("CloudTechnologies_CustomerOrderManagement"));
                    Assert.assertTrue(matchedResult.containsKey("AlexaGBUsers"));
                    Assert.assertTrue(matchedResult.containsKey("CloudTechnologies_WCMS"));
                    Assert.assertTrue(matchedResult.containsKey("AlexaAUUsers"));
                } else {
                    Assert.assertTrue(false, columnSelection.toString());
                }
            }
        }
    }

    private Map<RecordModelTuple, Map<String, Object>> extractMap(
            Map<RecordModelTuple, Map<String, Map<String, Object>>> unorderedMatchedRecordEnrichmentMap, String key) {
        Map<RecordModelTuple, Map<String, Object>> map = new HashMap<>();
        for (RecordModelTuple tupleKey : unorderedMatchedRecordEnrichmentMap.keySet()) {
            Map<String, Object> dataMap = unorderedMatchedRecordEnrichmentMap.get(tupleKey).get(key);
            if (dataMap != null) {
                map.put(tupleKey, dataMap);
            }
        }
        return map;
    }
}
