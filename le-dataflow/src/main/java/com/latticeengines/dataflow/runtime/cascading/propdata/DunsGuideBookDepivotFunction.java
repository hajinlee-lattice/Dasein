package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DunsGuideBookDepivotFunction extends BaseOperation implements Function {

    private static final long serialVersionUID = -2787583231529931537L;

    private int dunsLoc;
    private int targetDunsLoc;
    private int keyPartitionLoc;
    private int bookSourceLoc;

    public DunsGuideBookDepivotFunction(Fields fieldDeclarations) {
        super(fieldDeclarations);
        Map<String, Integer> namePositionMap = getPositionMap(fieldDeclaration);
        dunsLoc = namePositionMap.get(DunsGuideBookConfig.DUNS);
        targetDunsLoc = namePositionMap.get(DunsRedirectBookConfig.TARGET_DUNS);
        keyPartitionLoc = namePositionMap.get(DunsRedirectBookConfig.KEY_PARTITION);
        bookSourceLoc = namePositionMap.get(DunsRedirectBookConfig.BOOK_SOURCE);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String itemStr = arguments.getString(DunsGuideBookConfig.ITEMS);
        List<?> list = JsonUtils.deserialize(itemStr, List.class);
        List<DunsGuideBook.Item> books = JsonUtils.convertList(list, DunsGuideBook.Item.class);
        for (DunsGuideBook.Item item : books) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(dunsLoc, arguments.getString(DunsGuideBookConfig.DUNS));
            result.set(targetDunsLoc, item.getDuns());
            result.set(keyPartitionLoc, item.getKeyPartition());
            result.set(bookSourceLoc, item.getBookSource());
            functionCall.getOutputCollector().add(result);
        }
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
