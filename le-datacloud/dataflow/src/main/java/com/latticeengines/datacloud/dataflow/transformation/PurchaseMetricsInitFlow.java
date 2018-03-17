package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(PurchaseMetricsInitFlow.BEAN_NAME)
public class PurchaseMetricsInitFlow extends ConfigurableFlowBase<TransformerConfig> {

    public static final String BEAN_NAME = "purchaseMetricsInitFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node periodTable = addSource(parameters.getBaseTables().get(0));
        Node account = addSource(parameters.getBaseTables().get(1));
        // TODO: Wait for @Ke's change, then remove commented part
        //Node product = addSource(parameters.getBaseTables().get(2));

        List<String> retainFields = new ArrayList<>();
        retainFields.addAll(periodTable.getFieldNames());
        retainFields.add(InterfaceName.SpendAnalyticsSegment.name());

        periodTable = periodTable.join(new FieldList(InterfaceName.AccountId.name()), account,
                new FieldList(InterfaceName.AccountId.name()), JoinType.LEFT)
                /* TODO: Wait for @Ke's change, then remove commented part
                .join(new FieldList(InterfaceName.ProductId.name()), product,
                        new FieldList(InterfaceName.ProductId.name()), JoinType.LEFT)
                .filter(String.format("%s != null && \"%s\".equalsIgnoreCase(%s)", InterfaceName.SpendAnalyticsSegment,
                        ProductType.ANALYTIC, InterfaceName.ProductType),
                        new FieldList(InterfaceName.ProductType.name(), InterfaceName.SpendAnalyticsSegment.name()))
                        */
                .retain(new FieldList(retainFields));

        return periodTable;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return PurchaseMetricsInitFlow.BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PURCHASE_METRICS_INITIATOR;
    }
}
