package com.latticeengines.datacloud.dataflow.transformation.seed;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_EMPLOYEE_HERE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_EMPLOYEE_HERE_CODE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_EMPLOYEE_TOTAL;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_EMPLOYEE_TOTAL_CODE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_SALES_VOL_US;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_SALES_VOL_US_CODE;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

/**
 * A pipeline step in DnBCacheSeed rebuild pipeline
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-DnBCacheSeedCreation
 */
@Component(DnBCleanFlow.DATAFLOW_BEAN_NAME)
public class DnBCleanFlow extends ConfigurableFlowBase<TransformerConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DnBCleanFLow";

    public static final String TRANSFORMER_NAME = "DnBCleanTransformer";

    private static final String NEW = "NEW_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        FieldMetadata salesFm = source.getSchema(ATTR_SALES_VOL_US);
        String expr = String.format("(%s == null || (%s == 0 && (%s == null || \"2\".equals(%s)))) ? null : %s",
                ATTR_SALES_VOL_US, ATTR_SALES_VOL_US, ATTR_SALES_VOL_US_CODE, ATTR_SALES_VOL_US_CODE,
                ATTR_SALES_VOL_US);
        source = source
                .apply(expr, new FieldList(ATTR_SALES_VOL_US, ATTR_SALES_VOL_US_CODE),
                        new FieldMetadata(NEW + ATTR_SALES_VOL_US, salesFm.getJavaType()))
                .discard(new FieldList(ATTR_SALES_VOL_US))
                .rename(new FieldList(NEW + ATTR_SALES_VOL_US), new FieldList(ATTR_SALES_VOL_US));

        FieldMetadata empHereFm = source.getSchema(ATTR_EMPLOYEE_HERE);
        expr = String.format("(%s == null || (%s == 0 && (%s == null || \"2\".equals(%s)))) ? null : %s",
                ATTR_EMPLOYEE_HERE, ATTR_EMPLOYEE_HERE, ATTR_EMPLOYEE_HERE_CODE, ATTR_EMPLOYEE_HERE_CODE,
                ATTR_EMPLOYEE_HERE);
        source = source
                .apply(expr, new FieldList(ATTR_EMPLOYEE_HERE, ATTR_EMPLOYEE_HERE_CODE),
                        new FieldMetadata(NEW + ATTR_EMPLOYEE_HERE, empHereFm.getJavaType()))
                .discard(new FieldList(ATTR_EMPLOYEE_HERE))
                .rename(new FieldList(NEW + ATTR_EMPLOYEE_HERE), new FieldList(ATTR_EMPLOYEE_HERE));

        FieldMetadata empTotalFm = source.getSchema(ATTR_EMPLOYEE_TOTAL);
        expr = String.format("(%s == null || (%s == 0 && (%s == null || \"2\".equals(%s)))) ? null : %s",
                ATTR_EMPLOYEE_TOTAL, ATTR_EMPLOYEE_TOTAL, ATTR_EMPLOYEE_TOTAL_CODE, ATTR_EMPLOYEE_TOTAL_CODE,
                ATTR_EMPLOYEE_TOTAL);
        source = source
                .apply(expr, new FieldList(ATTR_EMPLOYEE_TOTAL, ATTR_EMPLOYEE_TOTAL_CODE),
                        new FieldMetadata(NEW + ATTR_EMPLOYEE_TOTAL, empTotalFm.getJavaType()))
                .discard(new FieldList(ATTR_EMPLOYEE_TOTAL))
                .rename(new FieldList(NEW + ATTR_EMPLOYEE_TOTAL), new FieldList(ATTR_EMPLOYEE_TOTAL));
        return source;
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
