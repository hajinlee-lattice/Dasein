package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DunsGuideBookAggregator extends BaseAggregator<DunsGuideBookAggregator.Context>
        implements Aggregator<DunsGuideBookAggregator.Context> {

    private static final long serialVersionUID = 4226231901493856121L;

    private Map<String, Integer> bookPriority;
    private int dunsIdx;
    private int itemsIdx;

    public static class Context extends BaseAggregator.Context {
        List<DunsGuideBook.Item> guideBooks;
        String duns;
    }

    public DunsGuideBookAggregator(Fields fieldDeclaration, Map<String, Integer> bookPriority) {
        super(fieldDeclaration);
        this.bookPriority = bookPriority;
        this.dunsIdx = namePositionMap.get(DunsGuideBookConfig.DUNS);
        this.itemsIdx = namePositionMap.get(DunsGuideBookConfig.ITEMS);
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(DunsRedirectBookConfig.DUNS);
        if (grpObj == null) {
            return true;
        }
        return false;
    }

    @Override
    protected Context initializeContext(TupleEntry group) {
        Context context = new Context();
        context.guideBooks = new ArrayList<>();
        return context;
    }

    @Override
    protected Context updateContext(Context context, TupleEntry arguments) {
        if (context.duns == null) {
            context.duns = arguments.getString(DunsRedirectBookConfig.DUNS);
        }
        String targetDuns = arguments.getString(DunsRedirectBookConfig.TARGET_DUNS);
        String keyPartition = arguments.getString(DunsRedirectBookConfig.KEY_PARTITION);
        String bookSource = arguments.getString(DunsRedirectBookConfig.BOOK_SOURCE);
        context.guideBooks.add(new DunsGuideBook.Item(targetDuns, keyPartition, bookSource));
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        // KeyPartition -> (TargetDuns, BookSource)
        Map<String, Pair<String, String>> map = new HashMap<>();
        context.guideBooks.forEach(guideBook -> {
            // Choose DunsRedirectBook with higher priority (lower value)
            if (!map.containsKey(guideBook.getKeyPartition())
                    || bookPriority.get(map.get(guideBook.getKeyPartition()).getRight()) > bookPriority
                            .get(guideBook.getBookSource())) {
                map.put(guideBook.getKeyPartition(), Pair.of(guideBook.getDuns(), guideBook.getBookSource()));
            }
        });
        List<DunsGuideBook.Item> finalGuideBook = new ArrayList<>();
        // KeyPartition -> (TargetDuns, BookSource)
        for (Map.Entry<String, Pair<String, String>> ent : map.entrySet()) {
            finalGuideBook
                    .add(new DunsGuideBook.Item(ent.getValue().getLeft(), ent.getKey(), ent.getValue().getRight()));
        }

        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(dunsIdx, context.duns);
        result.set(itemsIdx, JsonUtils.serialize(finalGuideBook));
        return result;
    }

}
