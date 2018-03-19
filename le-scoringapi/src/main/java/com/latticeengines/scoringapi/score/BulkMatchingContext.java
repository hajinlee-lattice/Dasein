package com.latticeengines.scoringapi.score;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.scoringapi.exposed.ScoringArtifacts;
import com.latticeengines.scoringapi.score.impl.RecordModelTuple;

public class BulkMatchingContext {
    private Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap = new HashMap<>();
    private Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap = new HashMap<>();
    private Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap = new HashMap<>();
    private Map<RecordModelTuple, List<String>> unorderedMatchLogMap = new HashMap<>();
    private Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap = new HashMap<>();
    private Map<RecordModelTuple, Map<String, Object>> matchedRecords = new HashMap<>();
    private Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap = new HashMap<>();
    private Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords = new HashMap<>();
    private List<RecordModelTuple> originalOrderParsedTupleList = new ArrayList<>();
    private List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList = new ArrayList<>();
    private List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList = new ArrayList<>();
    private List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList = new ArrayList<>();
    private List<RecordModelTuple> partiallyOrderedBadRecordList = new ArrayList<>();
    private List<ModelSummary> originalOrderModelSummaryList = new ArrayList<>();

    public static BulkMatchingContext instance() {
        return new BulkMatchingContext();
    }

    public Map<String, Map<String, FieldSchema>> getUniqueFieldSchemasMap() {
        return uniqueFieldSchemasMap;
    }

    public BulkMatchingContext setUniqueFieldSchemasMap(Map<String, Map<String, FieldSchema>> uniqueFieldSchemasMap) {
        this.uniqueFieldSchemasMap = uniqueFieldSchemasMap;
        return this;
    }

    public List<RecordModelTuple> getOriginalOrderParsedTupleList() {
        return originalOrderParsedTupleList;
    }

    public BulkMatchingContext setOriginalOrderParsedTupleList(List<RecordModelTuple> originalOrderParsedTupleList) {
        this.originalOrderParsedTupleList = originalOrderParsedTupleList;
        return this;
    }

    public List<RecordModelTuple> getPartiallyOrderedParsedRecordWithMatchReqList() {
        return partiallyOrderedParsedRecordWithMatchReqList;
    }

    public BulkMatchingContext setPartiallyOrderedParsedRecordWithMatchReqList(
            List<RecordModelTuple> partiallyOrderedParsedRecordWithMatchReqList) {
        this.partiallyOrderedParsedRecordWithMatchReqList = partiallyOrderedParsedRecordWithMatchReqList;
        return this;
    }

    public List<RecordModelTuple> getPartiallyOrderedParsedRecordWithoutMatchReqList() {
        return partiallyOrderedParsedRecordWithoutMatchReqList;
    }

    public BulkMatchingContext setPartiallyOrderedParsedRecordWithoutMatchReqList(
            List<RecordModelTuple> partiallyOrderedParsedRecordWithoutMatchReqList) {
        this.partiallyOrderedParsedRecordWithoutMatchReqList = partiallyOrderedParsedRecordWithoutMatchReqList;
        return this;
    }

    public List<RecordModelTuple> getPartiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList() {
        return partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList;
    }

    public BulkMatchingContext setPartiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList(
            List<RecordModelTuple> partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList) {
        this.partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList = partiallyOrderedParsedRecordWithEnrichButWithoutMatchReqList;
        return this;
    }

    public List<RecordModelTuple> getPartiallyOrderedBadRecordList() {
        return partiallyOrderedBadRecordList;
    }

    public BulkMatchingContext setPartiallyOrderedBadRecordList(List<RecordModelTuple> partiallyOrderedBadRecordList) {
        this.partiallyOrderedBadRecordList = partiallyOrderedBadRecordList;
        return this;
    }

    public List<ModelSummary> getOriginalOrderModelSummaryList() {
        return originalOrderModelSummaryList;
    }

    public BulkMatchingContext setOriginalOrderModelSummaryList(List<ModelSummary> originalOrderModelSummaryList) {
        this.originalOrderModelSummaryList = originalOrderModelSummaryList;
        return this;
    }

    public Map<RecordModelTuple, Map<String, Object>> getUnorderedCombinedRecordMap() {
        return unorderedCombinedRecordMap;
    }

    public BulkMatchingContext setUnorderedCombinedRecordMap(
            Map<RecordModelTuple, Map<String, Object>> unorderedCombinedRecordMap) {
        this.unorderedCombinedRecordMap = unorderedCombinedRecordMap;
        return this;
    }

    public Map<RecordModelTuple, Map<String, Object>> getUnorderedLeadEnrichmentMap() {
        return unorderedLeadEnrichmentMap;
    }

    public BulkMatchingContext setUnorderedLeadEnrichmentMap(
            Map<RecordModelTuple, Map<String, Object>> unorderedLeadEnrichmentMap) {
        this.unorderedLeadEnrichmentMap = unorderedLeadEnrichmentMap;
        return this;
    }

    public Map<RecordModelTuple, List<String>> getUnorderedMatchLogMap() {
        return unorderedMatchLogMap;
    }

    public BulkMatchingContext setUnorderedMatchLogMap(Map<RecordModelTuple, List<String>> unorderedMatchLogMap) {
        this.unorderedMatchLogMap = unorderedMatchLogMap;
        return this;
    }

    public Map<RecordModelTuple, List<String>> getUnorderedMatchErrorLogMap() {
        return unorderedMatchErrorLogMap;
    }

    public BulkMatchingContext setUnorderedMatchErrorLogMap(
            Map<RecordModelTuple, List<String>> unorderedMatchErrorLogMap) {
        this.unorderedMatchErrorLogMap = unorderedMatchErrorLogMap;
        return this;
    }

    public Map<RecordModelTuple, Map<String, Object>> getMatchedRecords() {
        return matchedRecords;
    }

    public BulkMatchingContext setMatchedRecords(Map<RecordModelTuple, Map<String, Object>> matchedRecords) {
        this.matchedRecords = matchedRecords;
        return this;
    }

    public Map<String, Entry<LedpException, ScoringArtifacts>> getUniqueScoringArtifactsMap() {
        return uniqueScoringArtifactsMap;
    }

    public BulkMatchingContext setUniqueScoringArtifactsMap(
            Map<String, Entry<LedpException, ScoringArtifacts>> uniqueScoringArtifactsMap) {
        this.uniqueScoringArtifactsMap = uniqueScoringArtifactsMap;
        return this;
    }

    public Map<RecordModelTuple, Map<String, Object>> getUnorderedTransformedRecords() {
        return unorderedTransformedRecords;
    }

    public BulkMatchingContext setUnorderedTransformedRecords(
            Map<RecordModelTuple, Map<String, Object>> unorderedTransformedRecords) {
        this.unorderedTransformedRecords = unorderedTransformedRecords;
        return this;
    }

}
