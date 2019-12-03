package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.BaseAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class DunsGuideBookAggregator extends BaseAggregator<DunsGuideBookAggregator.Context>
        implements Aggregator<DunsGuideBookAggregator.Context> {

    private static final long serialVersionUID = 4226231901493856121L;

    // DunsGuideBook fields
    private static final String GB_DUNS = DunsGuideBook.SRC_DUNS_KEY;
    private static final String GB_ITEMS = DunsGuideBook.ITEMS_KEY;

    // DunsRedirectBook fields
    private static final String RB_DUNS = DunsRedirectBookConfig.DUNS;
    private static final String RB_TGT_DUNS = DunsRedirectBookConfig.TARGET_DUNS;
    private static final String RB_KEY_PAR = DunsRedirectBookConfig.KEY_PARTITION;
    private static final String RB_BOOK_SRC = DunsRedirectBookConfig.BOOK_SOURCE;

    // AMSeed fields
    private static final String AMS_DU_DUNS = DataCloudConstants.ATTR_DU_DUNS;
    private static final String AMS_GU_DUNS = DataCloudConstants.ATTR_GU_DUNS;

    private Map<String, Integer> bookPriority;
    private int dunsIdx;
    private int itemsIdx;

    public DunsGuideBookAggregator(Fields fieldDeclaration, Map<String, Integer> bookPriority) {
        super(fieldDeclaration);
        this.bookPriority = bookPriority;
        this.dunsIdx = namePositionMap.get(GB_DUNS);
        this.itemsIdx = namePositionMap.get(GB_ITEMS);
    }

    public static class Context extends BaseAggregator.Context {
        List<DunsGuideBook.Item> guideBooks;
        String duns;
    }

    @Override
    protected boolean isDummyGroup(TupleEntry group) {
        Object grpObj = group.getObject(RB_DUNS);
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
            context.duns = arguments.getString(RB_DUNS);
        }
        String targetDuns = arguments.getString(RB_TGT_DUNS);
        String targetDuDuns = arguments.getString(AMS_DU_DUNS);
        String targetGuDuns = arguments.getString(AMS_GU_DUNS);
        String keyPartition = arguments.getString(RB_KEY_PAR);
        String bookSource = arguments.getString(RB_BOOK_SRC);
        context.guideBooks
                .add(new DunsGuideBook.Item(targetDuns, targetDuDuns, targetGuDuns, keyPartition, bookSource));
        return context;
    }

    @Override
    protected Tuple finalizeContext(Context context) {
        // KeyPartition -> DunsGuideBook.Item
        Map<String, DunsGuideBook.Item> map = new HashMap<>();
        context.guideBooks.forEach(guideBook -> {
            // Choose DunsRedirectBook with higher priority (lower value)
            if (!map.containsKey(guideBook.getKeyPartition()) //
                    || bookPriority.get(map.get(guideBook.getKeyPartition()).getBookSource()) > bookPriority
                            .get(guideBook.getBookSource())) {
                map.put(guideBook.getKeyPartition(), guideBook);
            }
        });
        Tuple result = Tuple.size(getFieldDeclaration().size());
        result.set(dunsIdx, context.duns);
        result.set(itemsIdx, JsonUtils.serialize(map.values()));
        return result;
    }

}
