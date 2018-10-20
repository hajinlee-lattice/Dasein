package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.Map;

import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBookConfig;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DunsGuideBookNLEnrichFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -8506196845291044144L;

    private int dunsLoc;
    private int targetDunsLoc;
    private int keyPartitionLoc;
    private int bookSourceLoc;
    private int nameLoc;
    private int countryLoc;
    private int stateLoc;
    private int cityLoc;

    public DunsGuideBookNLEnrichFunction(Fields fieldDeclarations) {
        super(fieldDeclarations);
        Map<String, Integer> namePositionMap = getPositionMap(fieldDeclaration);
        nameLoc = namePositionMap.get(MatchKey.Name.name());
        countryLoc = namePositionMap.get(MatchKey.Country.name());
        stateLoc = namePositionMap.get(MatchKey.State.name());
        cityLoc = namePositionMap.get(MatchKey.City.name());
        dunsLoc = namePositionMap.get(DunsGuideBookConfig.DUNS);
        targetDunsLoc = namePositionMap.get(DunsRedirectBookConfig.TARGET_DUNS);
        keyPartitionLoc = namePositionMap.get(DunsRedirectBookConfig.KEY_PARTITION);
        bookSourceLoc = namePositionMap.get(DunsRedirectBookConfig.BOOK_SOURCE);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String keyPartition = arguments.getString(DunsRedirectBookConfig.KEY_PARTITION);
        String name = keyPartition.contains(MatchKey.Name.name())
                ? arguments.getString(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Name)) : null;
        String country = keyPartition.contains(MatchKey.Country.name())
                ? arguments.getString(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.Country)) : null;
        String state = keyPartition.contains(MatchKey.State.name())
                ? arguments.getString(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.State)) : null;
        String city = keyPartition.contains(MatchKey.City.name())
                ? arguments.getString(MatchKeyUtils.AM_FIELD_MAP.get(MatchKey.City)) : null;

        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(nameLoc, name);
        result.set(countryLoc, country);
        result.set(stateLoc, state);
        result.set(cityLoc, city);
        result.set(dunsLoc, arguments.getString(DunsGuideBookConfig.DUNS));
        result.set(targetDunsLoc, arguments.getString(DunsRedirectBookConfig.TARGET_DUNS));
        result.set(keyPartitionLoc, arguments.getString(DunsRedirectBookConfig.KEY_PARTITION));
        result.set(bookSourceLoc, arguments.getString(DunsRedirectBookConfig.BOOK_SOURCE));
        functionCall.getOutputCollector().add(result);
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
