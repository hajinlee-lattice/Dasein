package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DunsRedirectFromDomDunsMapFunc extends BaseOperation implements Function {

    private static final long serialVersionUID = 1618206727776198248L;

    private String bookSource;
    private String priDunsField;
    private String priCtryDunsField;
    private String priStDunsField;
    private int dunsLoc;
    private int targetDunsLoc;
    private int keyPartitionLoc;
    private int bookSourceLoc;

    public DunsRedirectFromDomDunsMapFunc(Fields fieldDeclaration, String bookSource,
            String priDunsField, String priCtryDunsField, String priStDunsField) {
        super(fieldDeclaration);
        Map<String, Integer> namePositionMap = getPositionMap(fieldDeclaration);
        this.bookSource = bookSource;
        this.priDunsField = priDunsField;
        this.priCtryDunsField = priCtryDunsField;
        this.priStDunsField = priStDunsField;
        this.dunsLoc = namePositionMap.get(DunsRedirectBookConfig.DUNS);
        this.targetDunsLoc = namePositionMap.get(DunsRedirectBookConfig.TARGET_DUNS);
        this.keyPartitionLoc = namePositionMap.get(DunsRedirectBookConfig.KEY_PARTITION);
        this.bookSourceLoc = namePositionMap.get(DunsRedirectBookConfig.BOOK_SOURCE);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String duns = arguments.getString(DataCloudConstants.ATTR_LDC_DUNS);
        String priDuns = arguments.getString(priDunsField);
        String priCtryDuns = arguments.getString(priCtryDunsField);
        String priStDuns = arguments.getString(priStDunsField);
        // duns is guaranteed to be non-empty, other primary duns are not
        if (!duns.equals(priDuns) && StringUtils.isNotBlank(priDuns)) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            String[] keys = { MatchKey.Name.name() };
            result = updateResult(result, duns, priDuns,
                    MatchKeyUtils.buildKeyPartition(Arrays.asList(keys)));
            functionCall.getOutputCollector().add(result);
        }
        if (!duns.equals(priCtryDuns) && StringUtils.isNotBlank(priCtryDuns)) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            String[] keys = { MatchKey.Name.name(), MatchKey.Country.name() };
            result = updateResult(result, duns, priCtryDuns,
                    MatchKeyUtils.buildKeyPartition(Arrays.asList(keys)));
            functionCall.getOutputCollector().add(result);
        }
        if (!duns.equals(priStDuns) && StringUtils.isNotBlank(priStDuns)) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            String[] keys = { MatchKey.Name.name(), MatchKey.Country.name(),
                    MatchKey.State.name() };
            result = updateResult(result, duns, priStDuns,
                    MatchKeyUtils.buildKeyPartition(Arrays.asList(keys)));
            functionCall.getOutputCollector().add(result);
        }
    }

    private Tuple updateResult(Tuple result, String duns, String targetDuns, String keyPartition) {
        result.set(dunsLoc, duns);
        result.set(targetDunsLoc, targetDuns);
        result.set(keyPartitionLoc, keyPartition);
        result.set(bookSourceLoc, bookSource);
        return result;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }
}
