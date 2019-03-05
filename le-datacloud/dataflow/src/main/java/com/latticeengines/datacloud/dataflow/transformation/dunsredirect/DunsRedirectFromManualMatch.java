package com.latticeengines.datacloud.dataflow.transformation.dunsredirect;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.ValidationUtils;
import com.latticeengines.common.exposed.validator.BeanValidationService;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectBookConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.DunsRedirectFromManualMatchConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

/**
 * Generate DunsRedirectBook rows from bulk match enriched ManualSeedStandard data
 */
@Component(DunsRedirectFromManualMatch.DATAFLOW_BEAN_NAME)
public class DunsRedirectFromManualMatch
        extends ConfigurableFlowBase<DunsRedirectFromManualMatchConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DunsRedirectFromManualMatch";
    public static final String TRANSFORMER_NAME = "DunsRedirectFromManualMatchTransformer";

    @Inject
    private BeanValidationService beanValidationService;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        DunsRedirectFromManualMatchConfig config = getTransformerConfig(parameters);
        ValidationUtils.check(beanValidationService, config, DunsRedirectFromManualMatchConfig.class.getName());

        Node source = addSource(parameters.getBaseTables().get(0));

        // column names
        String manDuns = config.getManualSeedDuns();
        String keyPartition = DunsRedirectBookConfig.KEY_PARTITION;
        String srcDuns = DunsRedirectBookConfig.DUNS;
        String targetDuns = DunsRedirectBookConfig.TARGET_DUNS;
        String bookSource = DunsRedirectBookConfig.BOOK_SOURCE;
        String salesInB = config.getSalesInBillions();
        String totalEmployees = config.getTotalEmployees();

        FieldList srcFieldList = getSourceFieldList(config);
        source = preFilter(source, config)
                // filter out invalid source rows and retain only required fields
                .retain(srcFieldList)
                // dedup by [LDC_DUNS,KeyPartition], resolve duplicates by higher salesInBillion and then totalEmployees
                // null will be at the end
                .groupByAndLimit(
                        new FieldList(DataCloudConstants.ATTR_LDC_DUNS, keyPartition),
                        new FieldList(salesInB, totalEmployees),
                        1, true, false)
                .rename(new FieldList(DataCloudConstants.ATTR_LDC_DUNS, manDuns),
                        new FieldList(srcDuns, targetDuns))
                // add BookSource
                .addColumnWithFixedValue(bookSource, config.getBookSource(), String.class)
                .retain(srcDuns, targetDuns, keyPartition, bookSource);

        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return DunsRedirectFromManualMatchConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    private FieldList getSourceFieldList(@NotNull DunsRedirectFromManualMatchConfig config) {
        return new FieldList(getSourceFieldNames(config));
    }

    /*
     * required source columns
     */
    private String[] getSourceFieldNames(@NotNull DunsRedirectFromManualMatchConfig config) {
        return new String[] {
                config.getManualSeedDuns(), DunsRedirectBookConfig.KEY_PARTITION, config.getSalesInBillions(),
                config.getTotalEmployees(), DataCloudConstants.ATTR_LDC_DUNS
        };
    }

    private Node preFilter(@NotNull Node source, @NotNull DunsRedirectFromManualMatchConfig config) {
        String manDuns = config.getManualSeedDuns();
        String ldcDuns = DataCloudConstants.ATTR_LDC_DUNS;
        // NOTE ManDuns should not be null, just in case
        // ManDuns != null && LDC_DUNS != null
        // NOTE allow ManDuns == LDC_DUNS to prevent other book source from redirecting ManDuns to other DUNS
        String exp = String.format("%s != null && %s != null", manDuns, ldcDuns);
        return source.filter(exp, new FieldList(manDuns, ldcDuns));
    }
}
