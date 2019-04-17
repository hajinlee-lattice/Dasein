package com.latticeengines.datacloud.dataflow.transformation.dunsredirect;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DunsGuideBookDepivotFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DunsGuideBookNLEnrichFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(DunsGuideBookDepivot.DATAFLOW_BEAN_NAME)
public class DunsGuideBookDepivot extends ConfigurableFlowBase<TransformerConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DunsGuideBookDepivotFlow";
    public static final String TRANSFORMER_NAME = "DunsGuideBookDepivot";

    // DunsGuideBook fields
    private static final String GB_DUNS = DunsGuideBook.SRC_DUNS_KEY;
    private static final String GB_ITEMS = DunsGuideBook.ITEMS_KEY;

    // DunsRedirectBook fields
    private static final String RB_TGT_DUNS = DunsRedirectBookConfig.TARGET_DUNS;
    private static final String RB_KEY_PAR = DunsRedirectBookConfig.KEY_PARTITION;
    private static final String RB_BOOK_SRC = DunsRedirectBookConfig.BOOK_SOURCE;

    // AMSeed fields
    private static final String AMS_DUNS = DataCloudConstants.ATTR_LDC_DUNS;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node book = addSource(parameters.getBaseTables().get(0));
        Node ams = addSource(parameters.getBaseTables().get(1));

        // Get all duns with target duns
        book = book.filter(GB_ITEMS + " != null", new FieldList(GB_ITEMS));

        // Depivot items
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(RB_TGT_DUNS, String.class));
        fms.add(new FieldMetadata(RB_KEY_PAR, String.class));
        fms.add(new FieldMetadata(RB_BOOK_SRC, String.class));
        List<String> fields = new ArrayList<>(Arrays.asList(GB_DUNS, RB_TGT_DUNS, RB_KEY_PAR, RB_BOOK_SRC));
        Node bookDepivoted = book.apply(
                new DunsGuideBookDepivotFunction(new Fields(fields.toArray(new String[fields.size()]))),
                new FieldList(book.getFieldNames()),
                fms, new FieldList(fields), Fields.RESULTS);

        // Dedup AccountMasterSeed by duns so that when enriching name+location
        // to DunsGuideBookDepivoted, no duplication introduced
        ams = ams.groupByAndLimit(new FieldList(AMS_DUNS), 1);

        // Enrich name + location to bookDepivoted
        bookDepivoted = bookDepivoted.join(new FieldList(GB_DUNS), ams,
                new FieldList(AMS_DUNS), JoinType.LEFT);
        List<String> nlFields = Arrays.asList(MatchKey.Name.name(), MatchKey.Country.name(), MatchKey.State.name(),
                MatchKey.City.name());
        fields.addAll(nlFields);
        fms.clear();
        fms.add(new FieldMetadata(MatchKey.Name.name(), String.class));
        fms.add(new FieldMetadata(MatchKey.Country.name(), String.class));
        fms.add(new FieldMetadata(MatchKey.State.name(), String.class));
        fms.add(new FieldMetadata(MatchKey.City.name(), String.class));
        bookDepivoted = bookDepivoted.apply(
                new DunsGuideBookNLEnrichFunction(new Fields(fields.toArray(new String[fields.size()]))),
                new FieldList(bookDepivoted.getFieldNames()), fms, new FieldList(fields), Fields.RESULTS);

        return bookDepivoted;
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
        return TransformerConfig.class;
    }
}
