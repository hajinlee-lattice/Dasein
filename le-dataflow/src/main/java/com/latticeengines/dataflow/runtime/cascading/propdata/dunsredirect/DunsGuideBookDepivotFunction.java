package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.List;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseFunction;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;

import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class DunsGuideBookDepivotFunction extends BaseFunction {

    private static final long serialVersionUID = -2787583231529931537L;

    private static final String DUNS = DunsGuideBook.SRC_DUNS_KEY;
    private static final String ITEMS = DunsGuideBook.ITEMS_KEY;
    private static final String TGT_DUNS = DunsRedirectBookConfig.TARGET_DUNS;
    private static final String KEY_PAR = DunsRedirectBookConfig.KEY_PARTITION;
    private static final String BOOK_SRC = DunsRedirectBookConfig.BOOK_SOURCE;

    private int dunsLoc;
    private int targetDunsLoc;
    private int keyPartitionLoc;
    private int bookSourceLoc;

    public DunsGuideBookDepivotFunction(Fields fieldDeclarations) {
        super(fieldDeclarations);
        dunsLoc = namePositionMap.get(DUNS);
        targetDunsLoc = namePositionMap.get(TGT_DUNS);
        keyPartitionLoc = namePositionMap.get(KEY_PAR);
        bookSourceLoc = namePositionMap.get(BOOK_SRC);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry arguments = functionCall.getArguments();
        String itemStr = arguments.getString(ITEMS);
        List<?> list = JsonUtils.deserialize(itemStr, List.class);
        List<DunsGuideBook.Item> books = JsonUtils.convertList(list, DunsGuideBook.Item.class);
        for (DunsGuideBook.Item item : books) {
            Tuple result = Tuple.size(getFieldDeclaration().size());
            result.set(dunsLoc, arguments.getString(DUNS));
            result.set(targetDunsLoc, item.getDuns());
            result.set(keyPartitionLoc, item.getKeyPartition());
            result.set(bookSourceLoc, item.getBookSource());
            functionCall.getOutputCollector().add(result);
        }
    }
}
