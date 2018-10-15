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

    // Find all the duns from AMSeed
    private Node getFullDuns(Node ams) {
        return ams
                .filter(DataCloudConstants.ATTR_LDC_DUNS + " != null", new FieldList(DataCloudConstants.ATTR_LDC_DUNS))
                .retain(new FieldList(DataCloudConstants.ATTR_LDC_DUNS))
                .groupByAndLimit(new FieldList(DataCloudConstants.ATTR_LDC_DUNS), 1);
    }

    // Merge DunsRedirectBooks
    // For each duns, if there is conflict in target duns with same key parition
    // from different book source, choose from the book source with higher
    // priority (lower priority value)
    private Node mergeRedirectBooks(Node fullDuns, List<Node> books) {
        books = enforceSchema(books);

        Node mergedBook = books.get(0);
        if (books.size() > 1) {
            books.remove(0);
            mergedBook = mergedBook.merge(books);
        }

        // Remove target duns in merged DunsRedirectBook which does not exist in
        // AMSeed;
        // Source duns which does not exist in AMSeed are exclude in
        // enrichFullDuns() by joining with FullDuns
        List<String> toRetain = mergedBook.getFieldNames();
        mergedBook = mergedBook
                .join(new FieldList(DunsRedirectBookConfig.TARGET_DUNS), fullDuns,
                        new FieldList(DataCloudConstants.ATTR_LDC_DUNS), JoinType.INNER)
                .retain(new FieldList(toRetain));

        String[] fields = { DunsGuideBookConfig.DUNS, DunsGuideBookConfig.ITEMS };
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(DunsGuideBookConfig.DUNS, String.class));
        fms.add(new FieldMetadata(DunsGuideBookConfig.ITEMS, String.class));
        DunsGuideBookAggregator agg = new DunsGuideBookAggregator(new Fields(fields), config.getBookPriority());
        mergedBook = mergedBook.groupByAndAggregate(new FieldList(DunsRedirectBookConfig.DUNS), agg, fms);
        return mergedBook;
    }

    private List<Node> enforceSchema(List<Node> books) {
        List<Node> newBooks = new ArrayList<>();
        String[] fields = { DunsRedirectBookConfig.DUNS, DunsRedirectBookConfig.TARGET_DUNS,
                DunsRedirectBookConfig.KEY_PARTITION, DunsRedirectBookConfig.BOOK_SOURCE };
        books.forEach(book -> {
            book = book.retain(new FieldList(fields));
            newBooks.add(book);
        });
        return newBooks;
    }

    // Put all the duns from AMSeed in DunsGuideBook so DunsGuideBook can also
    // serve the purpose of verifying whether duns got from DnB exists in AMSeed
    private Node enrichFullDuns(Node fullDuns, Node guideBook) {
        guideBook = fullDuns
                .join(new FieldList(DataCloudConstants.ATTR_LDC_DUNS), guideBook,
                        new FieldList(DunsGuideBookConfig.DUNS), JoinType.LEFT) //
                .discard(new FieldList(DunsGuideBookConfig.DUNS)) //
                .rename(new FieldList(DataCloudConstants.ATTR_LDC_DUNS), new FieldList(DunsGuideBookConfig.DUNS));
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
