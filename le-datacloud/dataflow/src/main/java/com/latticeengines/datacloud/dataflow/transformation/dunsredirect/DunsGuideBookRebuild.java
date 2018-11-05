package com.latticeengines.datacloud.dataflow.transformation.dunsredirect;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DunsGuideBookAggregator;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(DunsGuideBookRebuild.DATAFLOW_BEAN_NAME)
public class DunsGuideBookRebuild extends ConfigurableFlowBase<DunsGuideBookConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DunsGuideBookRebuildFlow";
    public static final String TRANSFORMER_NAME = "DunsGuideBookRebuild";

    private DunsGuideBookConfig config;
    // AMSeed duns field
    private static final String AMS_DUNS = DataCloudConstants.ATTR_LDC_DUNS;
    // DunsRedirectBook duns field
    private static final String RB_DUNS = DunsRedirectBookConfig.DUNS;
    // DunsRedirectBook target duns field
    private static final String RB_TG_DUNS = DunsRedirectBookConfig.TARGET_DUNS;
    // DunsRedirectBook key partition field
    private static final String RB_KEY = DunsRedirectBookConfig.KEY_PARTITION;
    // DunsRedirectBook book source field
    private static final String RB_SRC = DunsRedirectBookConfig.BOOK_SOURCE;
    // DunsGuideBook duns field
    private static final String GB_DUNS = DunsGuideBookConfig.DUNS;
    // DunsGuideBook items field
    private static final String GB_ITEMS = DunsGuideBookConfig.ITEMS;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node ams = addSource(parameters.getBaseTables().get(0));
        List<Node> books = new ArrayList<>();
        for (int i = 0; i < config.getBookPriority().size(); i++) {
            Node book = addSource(parameters.getBaseTables().get(i + 1));
            books.add(book);
        }

        Node fullDuns = getFullDuns(ams);
        Node guideBook = mergeRedirectBooks(fullDuns, books);
        guideBook = enrichFullDuns(fullDuns, guideBook);
        return guideBook;
    }

    /**
     * Find all the duns from AMSeed
     * 
     * @param ams
     * @return
     */
    private Node getFullDuns(Node ams) {
        return ams
                .filter(AMS_DUNS + " != null", new FieldList(AMS_DUNS)).retain(new FieldList(AMS_DUNS))
                .groupByAndLimit(new FieldList(AMS_DUNS), 1);
    }

    /**
     * Merge DunsRedirectBooks
     * 
     * For each duns, if there is conflict in target duns with same key parition
     * from different book source, choose from the book source with higher
     * priority (lower priority value)
     * 
     * @param fullDuns
     * @param books
     * @return
     */
    private Node mergeRedirectBooks(Node fullDuns, List<Node> books) {
        books = enforceSchema(books);

        Node mergedBook = books.get(0);
        if (books.size() > 1) {
            books.remove(0);
            mergedBook = mergedBook.merge(books);
        }

        // Remove target duns in merged DunsRedirectBook which does not exist in
        // AMSeed;
        List<String> toRetain = mergedBook.getFieldNames();
        mergedBook = mergedBook
                .join(new FieldList(RB_TG_DUNS), fullDuns, new FieldList(AMS_DUNS), JoinType.INNER)
                .retain(new FieldList(toRetain));

        String[] fields = { GB_DUNS, GB_ITEMS };
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(GB_DUNS, String.class));
        fms.add(new FieldMetadata(GB_ITEMS, String.class));
        DunsGuideBookAggregator agg = new DunsGuideBookAggregator(new Fields(fields), config.getBookPriority());
        mergedBook = mergedBook.groupByAndAggregate(new FieldList(RB_DUNS), agg, fms);
        return mergedBook;
    }

    private List<Node> enforceSchema(List<Node> books) {
        List<Node> newBooks = new ArrayList<>();
        String[] fields = { RB_DUNS, RB_TG_DUNS, RB_KEY, RB_SRC };
        books.forEach(book -> {
            book = book.retain(new FieldList(fields));
            newBooks.add(book);
        });
        return newBooks;
    }

    /**
     * Source duns in DunsGuideBook should contains all the duns from AMSeed,
     * but they are allowed to be not existing in AMSeed
     * 
     * @param fullDuns
     * @param guideBook
     * @return
     */
    private Node enrichFullDuns(Node fullDuns, Node guideBook) {
        guideBook = fullDuns
                .join(new FieldList(AMS_DUNS), guideBook,
                        new FieldList(GB_DUNS), JoinType.OUTER) //
                // currently expression function does not support read multiple
                // fields and update one fields among them, can only generate a
                // new field first
                // ams duns != null ? ams duns : gb duns
                .apply(String.format("%s != null ? %s : %s", AMS_DUNS, AMS_DUNS, GB_DUNS),
                        new FieldList(AMS_DUNS, GB_DUNS), new FieldMetadata("_DUNS_TEMP_", String.class)) //
                .discard(new FieldList(AMS_DUNS, GB_DUNS)) //
                .rename(new FieldList("_DUNS_TEMP_"), new FieldList(GB_DUNS));
        return guideBook;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return DunsGuideBookConfig.class;
    }
}
