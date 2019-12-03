package com.latticeengines.dataflow.runtime.cascading.propdata;

import com.latticeengines.dataflow.runtime.cascading.BaseFunction;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DunsGuideBookNLEnrichFunction extends BaseFunction {

    private static final long serialVersionUID = -8506196845291044144L;

    private static final String DUNS = DunsGuideBook.SRC_DUNS_KEY;
    private static final String TGT_DUNS = DunsRedirectBookConfig.TARGET_DUNS;
    private static final String KEY_PAR = DunsRedirectBookConfig.KEY_PARTITION;
    private static final String BOOK_SRC = DunsRedirectBookConfig.BOOK_SOURCE;

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
        nameLoc = namePositionMap.get(MatchKey.Name.name());
        countryLoc = namePositionMap.get(MatchKey.Country.name());
        stateLoc = namePositionMap.get(MatchKey.State.name());
        cityLoc = namePositionMap.get(MatchKey.City.name());
        dunsLoc = namePositionMap.get(DUNS);
        targetDunsLoc = namePositionMap.get(TGT_DUNS);
        keyPartitionLoc = namePositionMap.get(KEY_PAR);
        bookSourceLoc = namePositionMap.get(BOOK_SRC);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String keyPartition = arguments.getString(KEY_PAR);
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
        result.set(dunsLoc, arguments.getString(DUNS));
        result.set(targetDunsLoc, arguments.getString(TGT_DUNS));
        result.set(keyPartitionLoc, arguments.getString(KEY_PAR));
        result.set(bookSourceLoc, arguments.getString(BOOK_SRC));
        functionCall.getOutputCollector().add(result);
    }
}
