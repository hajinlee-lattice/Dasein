package com.latticeengines.scoringapi.score.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugRecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse.ScoreModelTuple;
import com.latticeengines.scoringapi.controller.ScoringResourceDeploymentTestNGBase;
import com.latticeengines.scoringapi.controller.TestModelArtifactDataComposition;
import com.latticeengines.scoringapi.controller.TestModelConfiguration;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

public class SimpleScoreRequestProcessorDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {
    private static final int MAX_RECORD_COUNT = 19; // prime number for better
                                                    // distribution of models
    private ScoreRequestProcessor scoreRequestProcessor;
    private ScoreRequestProcessorImpl scoreRequestProcessorImpl;
    private List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList;
    private Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap = new HashMap<>();
    private Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap = new HashMap<>();
    private BulkRecordScoreRequest request;
    private List<RecordModelTuple> originalOrderParsedTupleList;
    private List<RecordModelTuple> partiallyOrderedParsedTupleList;
    private List<RecordModelTuple> partiallyOrderedPmmlParsedRecordList;
    private List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList;
    private List<RecordModelTuple> partiallyOrderedBadRecordList;
    private Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap;
    private Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords;
    private List<ModelSummary> originalOrderModelSummaryList;
    private Map<RecordModelTuple, Map<String, Object>> unorderedMatchedRecordMap;
    private Map<String, Map<String, Predefined>> recordModelIdSelectionMap;
    private Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap;

    public void init() throws IOException {
        scoreRequestProcessor = applicationContext.getBean("customScoreRequestProcessor", ScoreRequestProcessor.class);
        scoreRequestProcessorImpl = (ScoreRequestProcessorImpl) scoreRequestProcessor;
        modelList = createModelList();
    }

    @Override
    protected boolean shouldInit() {
        return false;
    }

    private void overwritePredifinedSelection() {
        int i = 0;
        for (String modelId : uniqueScoringArtifactsMap.keySet()) {
            ModelSummary modelSummary = uniqueScoringArtifactsMap.get(modelId).getValue().getModelSummary();
            Predefined predefinedSelection = (i++ % 2 == 0 ? Predefined.Model : Predefined.RTS);
            modelSummary.setPredefinedSelection(predefinedSelection);
            System.out.println(modelSummary.getId() + " -- " + predefinedSelection);
        }

        recordModelIdSelectionMap = new HashMap<>();

        for (Record record : request.getRecords()) {
            Map<String, Predefined> modelIdSelectionMap = new HashMap<>();

            for (String modelId : record.getModelAttributeValuesMap().keySet()) {
                modelIdSelectionMap.put(modelId,
                        uniqueScoringArtifactsMap.get(modelId).getValue().getModelSummary().getPredefinedSelection());
            }

            recordModelIdSelectionMap.put(record.getRecordId(), modelIdSelectionMap);
        }
        unorderedLeadEnrichmentMap = new HashMap<>();
    }

    @Test(groups = "deployment", enabled = true)
    public void testFetchModelArtifacts() throws IOException {
        init();
        request = getBulkScoreRequest(MAX_RECORD_COUNT, modelList, false);
        List<Record> records = request.getRecords();
        CustomerSpace space = ScoringApiControllerDeploymentTestNGBase.customerSpace;
        scoreRequestProcessorImpl.fetchModelArtifacts(space, records, uniqueScoringArtifactsMap, uniqueFieldSchemasMap);
        Assert.assertEquals(MAX_RECORD_COUNT, records.size());
        Assert.assertEquals(modelList.size(), uniqueScoringArtifactsMap.size());
        Assert.assertEquals(modelList.size(), uniqueFieldSchemasMap.size());
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
            originalOrderParsedTupleList = scoreRequestProcessorImpl.checkForMissingFields(uniqueScoringArtifactsMap,
                    uniqueFieldSchemasMap, bulkRequest, false);

            for (RecordModelTuple tuple : originalOrderParsedTupleList) {
                Assert.assertNull(tuple.getException());
            }
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testFetchModelArtifacts" })
    public void testCheckForMissingFields() throws IOException {
        originalOrderParsedTupleList = scoreRequestProcessorImpl.checkForMissingFields(uniqueScoringArtifactsMap,
                uniqueFieldSchemasMap, request, false);
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY, originalOrderParsedTupleList.size());

        int idx = 0;
        for (RecordModelTuple tuple : originalOrderParsedTupleList) {
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
        partiallyOrderedParsedTupleList = new ArrayList<>();
        partiallyOrderedPmmlParsedRecordList = new ArrayList<>();
        partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList = new ArrayList<>();

        scoreRequestProcessorImpl.extractParsedList(originalOrderParsedTupleList, uniqueScoringArtifactsMap,
                partiallyOrderedParsedTupleList, partiallyOrderedPmmlParsedRecordList,
                partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList, partiallyOrderedBadRecordList, false);

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testExtractParsedList" })
    public void testExtractModelSummaries() {
        originalOrderModelSummaryList = scoreRequestProcessorImpl.extractModelSummaries(originalOrderParsedTupleList,
                uniqueScoringArtifactsMap);
        Assert.assertNotNull(originalOrderModelSummaryList);
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY, originalOrderModelSummaryList.size());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testExtractModelSummaries" })
    public void testBulkMatchAndJoin() {
        unorderedMatchedRecordMap = simulateBulkMatchAndJoin();
        Assert.assertNotNull(unorderedMatchedRecordMap);
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY, unorderedMatchedRecordMap.size());

        checkMatchedResults();
    }

    private Map<RecordModelTuple, Map<String, Object>> simulateBulkMatchAndJoin() {
        Map<RecordModelTuple, Map<String, Object>> matchedData = new HashMap<>();
        for (RecordModelTuple tuple : partiallyOrderedParsedTupleList) {
            matchedData.put(tuple, tuple.getParsedData().getKey());
        }

        return matchedData;
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testBulkMatchAndJoin" })
    public void testAddMissingFields() {
        scoreRequestProcessorImpl.addMissingFields(uniqueFieldSchemasMap, unorderedMatchedRecordMap,
                originalOrderParsedTupleList);
        unorderedCombinedRecordMap = new HashMap<>();
        unorderedCombinedRecordMap.putAll(unorderedMatchedRecordMap);
        Assert.assertNotNull(unorderedCombinedRecordMap);
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY, unorderedCombinedRecordMap.size());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testAddMissingFields" })
    public void testTransform() {
        unorderedTransformedRecords = scoreRequestProcessorImpl.transform(uniqueScoringArtifactsMap,
                unorderedCombinedRecordMap, originalOrderParsedTupleList);

        Assert.assertNotNull(unorderedTransformedRecords);
        Assert.assertEquals(MAX_RECORD_COUNT * RECORD_MODEL_CARDINALITY, unorderedTransformedRecords.size());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testTransform" })
    public void testGenerateDebugScoreResponse() {
        List<RecordScoreResponse> recordScoreResponseDebugList = scoreRequestProcessorImpl.generateDebugScoreResponse(
                uniqueScoringArtifactsMap, unorderedTransformedRecords, originalOrderParsedTupleList,
                unorderedLeadEnrichmentMap, new HashMap<RecordModelTuple, List<String>>(),
                new HashMap<RecordModelTuple, List<String>>());

        Assert.assertNotNull(recordScoreResponseDebugList);
        Assert.assertEquals(MAX_RECORD_COUNT, recordScoreResponseDebugList.size());
        Assert.assertNotNull(recordScoreResponseDebugList.get(0).getScores().get(0).getProbability());
        Assert.assertNotNull(recordScoreResponseDebugList.get(0).getScores().get(0).getScore());
        Assert.assertTrue(recordScoreResponseDebugList.get(0).getScores().get(0).getProbability()
                .doubleValue() != recordScoreResponseDebugList.get(0).getScores().get(0).getScore().doubleValue());
        // Assert.assertNotNull(recordScoreResponseDebugList.get(0).getScores().get(0).getBucket());

        checkScoreResultList(recordScoreResponseDebugList, true);

    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "testGenerateDebugScoreResponse" })
    public void testGenerateScoreResponse() {
        List<RecordScoreResponse> recordScoreResponseList = scoreRequestProcessorImpl.generateScoreResponse(
                uniqueScoringArtifactsMap, unorderedTransformedRecords, originalOrderParsedTupleList,
                unorderedLeadEnrichmentMap, true, new HashMap<RecordModelTuple, List<String>>(),
                new HashMap<RecordModelTuple, List<String>>());

        Assert.assertNotNull(recordScoreResponseList);
        Assert.assertEquals(MAX_RECORD_COUNT, recordScoreResponseList.size());
        Assert.assertNotNull(recordScoreResponseList.get(0).getScores().get(0).getProbability());
        Assert.assertNotNull(recordScoreResponseList.get(0).getScores().get(0).getScore());
        Assert.assertTrue(recordScoreResponseList.get(0).getScores().get(0).getProbability()
                .doubleValue() != recordScoreResponseList.get(0).getScores().get(0).getScore().doubleValue());
        // Assert.assertNotNull(recordScoreResponseList.get(0).getScores().get(0).getBucket());

        checkScoreResultList(recordScoreResponseList, false);
    }

    private void checkScoreResultList(List<RecordScoreResponse> recordScoreResponseList, boolean isDebug) {
        for (RecordScoreResponse recordScoreResponse : recordScoreResponseList) {
            if (isDebug) {
                DebugRecordScoreResponse debugResponse = (DebugRecordScoreResponse) recordScoreResponse;
                Assert.assertNotNull(debugResponse.getTransformedRecordMap());
                Assert.assertTrue(debugResponse.getTransformedRecordMap().size() > 0);
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
                Assert.assertNotNull(matchedResult);
                String modelId = tuple.getModelId();

                Predefined columnSelection = uniqueScoringArtifactsMap.get(modelId).getValue().getModelSummary()
                        .getPredefinedSelection();

                Assert.assertEquals(modelIdSelectionMap.get(modelId), columnSelection);
            }
        }

    }
}