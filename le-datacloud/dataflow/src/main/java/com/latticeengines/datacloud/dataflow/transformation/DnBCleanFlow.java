package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(DnBCleanFlow.DATAFLOW_BEAN_NAME)
public class DnBCleanFlow extends ConfigurableFlowBase<TransformerConfig> {
    public static final String DATAFLOW_BEAN_NAME = "DnBCleanFLow";

    public static final String TRANSFORMER_NAME = "DnBCleanTransformer";

    private static final String NEW = "NEW_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        FieldMetadata salesFm = source.getSchema(DataCloudConstants.ATTR_SALES_VOL_US);
        String expr = String.format("(%s == null || (%s == 0 && (%s == null || \"2\".equals(%s)))) ? null : %s",
                DataCloudConstants.ATTR_SALES_VOL_US, DataCloudConstants.ATTR_SALES_VOL_US, //
                DataCloudConstants.ATTR_SALES_VOL_US_CODE, DataCloudConstants.ATTR_SALES_VOL_US_CODE, //
                DataCloudConstants.ATTR_SALES_VOL_US);
        source = source
                .apply(expr,
                        new FieldList(DataCloudConstants.ATTR_SALES_VOL_US, DataCloudConstants.ATTR_SALES_VOL_US_CODE),
                        new FieldMetadata(NEW + DataCloudConstants.ATTR_SALES_VOL_US, salesFm.getJavaType()))
                .discard(new FieldList(DataCloudConstants.ATTR_SALES_VOL_US))
                .rename(new FieldList(NEW + DataCloudConstants.ATTR_SALES_VOL_US),
                        new FieldList(DataCloudConstants.ATTR_SALES_VOL_US));

        FieldMetadata empHereFm = source.getSchema(DataCloudConstants.ATTR_EMPLOYEE_HERE);
        expr = String.format("(%s == null || (%s == 0 && (%s == null || \"2\".equals(%s)))) ? null : %s",
                DataCloudConstants.ATTR_EMPLOYEE_HERE, DataCloudConstants.ATTR_EMPLOYEE_HERE, //
                DataCloudConstants.ATTR_EMPLOYEE_HERE_CODE, DataCloudConstants.ATTR_EMPLOYEE_HERE_CODE, //
                DataCloudConstants.ATTR_EMPLOYEE_HERE);
        source = source
                .apply(expr, //
                        new FieldList(DataCloudConstants.ATTR_EMPLOYEE_HERE,
                                DataCloudConstants.ATTR_EMPLOYEE_HERE_CODE),
                        new FieldMetadata(NEW + DataCloudConstants.ATTR_EMPLOYEE_HERE, empHereFm.getJavaType()))
                .discard(new FieldList(DataCloudConstants.ATTR_EMPLOYEE_HERE))
                .rename(new FieldList(NEW + DataCloudConstants.ATTR_EMPLOYEE_HERE),
                        new FieldList(DataCloudConstants.ATTR_EMPLOYEE_HERE));

        FieldMetadata empTotalFm = source.getSchema(DataCloudConstants.ATTR_EMPLOYEE_TOTAL);
        expr = String.format("(%s == null || (%s == 0 && (%s == null || \"2\".equals(%s)))) ? null : %s",
                DataCloudConstants.ATTR_EMPLOYEE_TOTAL, DataCloudConstants.ATTR_EMPLOYEE_TOTAL, //
                DataCloudConstants.ATTR_EMPLOYEE_TOTAL_CODE, DataCloudConstants.ATTR_EMPLOYEE_TOTAL_CODE, //
                DataCloudConstants.ATTR_EMPLOYEE_TOTAL);
        source = source.apply(expr, //
                new FieldList(DataCloudConstants.ATTR_EMPLOYEE_TOTAL, DataCloudConstants.ATTR_EMPLOYEE_TOTAL_CODE),
                new FieldMetadata(NEW + DataCloudConstants.ATTR_EMPLOYEE_TOTAL, empTotalFm.getJavaType()))
                .discard(new FieldList(DataCloudConstants.ATTR_EMPLOYEE_TOTAL))
                .rename(new FieldList(NEW + DataCloudConstants.ATTR_EMPLOYEE_TOTAL),
                        new FieldList(DataCloudConstants.ATTR_EMPLOYEE_TOTAL));
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
