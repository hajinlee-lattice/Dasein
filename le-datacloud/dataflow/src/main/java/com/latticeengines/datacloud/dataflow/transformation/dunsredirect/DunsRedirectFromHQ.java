package com.latticeengines.datacloud.dataflow.transformation.dunsredirect;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ValidationUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

/**
 * Generate DunsRedirectBook rows from AccountMasterSeed's duns tree information
 */
@Component(DunsRedirectFromHQ.DATAFLOW_BEAN_NAME)
public class DunsRedirectFromHQ extends ConfigurableFlowBase<DunsRedirectBookConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DunsRedirectFromHQ";
    public static final String TRANSFORMER_NAME = "DunsRedirectFromHQTransformer";

    @Autowired
    private BeanValidationService beanValidationService;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        DunsRedirectBookConfig config = getTransformerConfig(parameters);
        ValidationUtils.check(beanValidationService, config, DunsRedirectBookConfig.class.getName());

        Node source = addSource(parameters.getBaseTables().get(0));

        // column names
        String ghqSalesVol = DataCloudConstants.ATTR_GLOBAL_HQ_SALES_VOL;
        String salesVol = DataCloudConstants.ATTR_SALES_VOL_US;
        String ldcDuns = DataCloudConstants.ATTR_LDC_DUNS;
        String guDuns = DataCloudConstants.ATTR_GU_DUNS;
        String keyPartition = DunsRedirectBookConfig.KEY_PARTITION;
        String srcDuns = DunsRedirectBookConfig.DUNS;
        String targetDuns = DunsRedirectBookConfig.TARGET_DUNS;
        String bookSource = DunsRedirectBookConfig.BOOK_SOURCE;

        source = preFilter(source, ldcDuns, guDuns, ghqSalesVol, salesVol)
                .retain(new FieldList(ldcDuns, guDuns, salesVol, ghqSalesVol))
                // dedup by LDC_DUNS, no need to sort because when LDC_DUNS is the same
                // all other required fields are guaranteed to be the same in AccountMasterSeed
                .groupByAndLimit(new FieldList(ldcDuns), 1)
                // rename to DunsRedirectBook fields
                .rename(new FieldList(ldcDuns, guDuns), new FieldList(srcDuns, targetDuns))
                .retain(new FieldList(srcDuns, targetDuns))
                // add KeyPartition and BookSource
                .addColumnWithFixedValue(keyPartition, getKeyPartition(), String.class)
                .addColumnWithFixedValue(bookSource, config.getBookSource(), String.class);

        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return DunsRedirectBookConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    private Node preFilter(@NotNull Node source, String ldcDuns, String guDuns, String ghqSalesVol, String salesVol) {
        // GU_DUNS != null && LDC_DUNS != null && GU_DUNS != LDC_DUNS && HQ_SALES_VOL >= SALES_VOL
        String exp = String.format("%s != null && %s != null && !%s.equals(%s) && (%s)",
                guDuns, ldcDuns, guDuns, ldcDuns, getSalesExpression(ghqSalesVol, salesVol));
        return source.filter(exp, new FieldList(guDuns, ldcDuns, ghqSalesVol, salesVol));
    }

    private String getSalesExpression(String ghqSalesVol, String salesVol) {
        // short version of the following expression
        // (HQ_VOL == null && VOL == null) || (HQ_VOL != null && VOL == null) ||
        // (HQ_VOL != null && VOL != null && HQ_VOL >= VOL)
        return String.format("%s == null || (%s != null && %s >= %s)", salesVol, ghqSalesVol, ghqSalesVol, salesVol);
    }

    /*
     * currently, only name is used
     */
    private String getKeyPartition() {
        MatchKeyTuple tuple = new MatchKeyTuple();
        // random value
        tuple.setName(DataCloudConstants.ATTR_LDC_NAME);
        return MatchKeyUtils.evalKeyPartition(tuple);
    }
}
