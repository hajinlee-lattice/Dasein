package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.EmployeeCleanUpFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.SalesCleanUpFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ManualSeedCleanTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(ManualSeedCleanFlow.DATAFLOW_BEAN_NAME)
public class ManualSeedCleanFlow
        extends ConfigurableFlowBase<ManualSeedCleanTransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "manualSeedCleanFlow";
    public static final String TRANSFORMER_NAME = "manualSeedCleanTransformer";

    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ManualSeedCleanTransformerConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node source = addSource(parameters.getBaseTables().get(0));
        ManualSeedCleanTransformerConfig config = getTransformerConfig(parameters);
        source = dollarInSalesCleanup(source, config.getSalesVolumeInUSDollars());
        source = totalEmployeesCleanup(source, config.getEmployeesTotal());

        if (!config.isNoDedup()) {
            // de-dup by duns and keep the one with highest sales volume
            source = source //
                    .groupByAndLimit(new FieldList(config.getManSeedDuns()),
                            new FieldList(config.getSalesVolumeInUSDollars()), 1, true, false);

            // de-dup by domain and keep the one with highest sales volume
            source = source //
                    .groupByAndLimit(new FieldList(config.getManSeedDomain()),
                            new FieldList(config.getSalesVolumeInUSDollars()), 1, true, false);
        }

        return source;
    }

    private Node dollarInSalesCleanup(Node source, String salesVolumnInBillions) {
        source = source.apply(new SalesCleanUpFunction(salesVolumnInBillions), new FieldList(salesVolumnInBillions),
                new FieldMetadata(salesVolumnInBillions, Long.class));
        return source;
    }

    private Node totalEmployeesCleanup(Node source, String employeesTotal) {
        source = source.apply(new EmployeeCleanUpFunction(employeesTotal), new FieldList(employeesTotal),
                new FieldMetadata(employeesTotal, Integer.class));
        return source;
    }

}
