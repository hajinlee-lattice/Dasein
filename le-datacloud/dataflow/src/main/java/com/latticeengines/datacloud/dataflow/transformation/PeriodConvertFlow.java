package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddCompositeColumnFuction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddPeriodColumnFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("periodConvertFlow")
public class PeriodConvertFlow extends ConsolidateBaseFlow<PeriodConvertorConfig> {

    public static final String DATAFLOW_BEAN_NAME = "periodConvertFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodConvertorConfig config = getTransformerConfig(parameters);

        Node result = addSource(parameters.getBaseTables().get(0));

        result = addPeriodIdColumn(config, result);

        result = addCompositeIdColumn(result);

        return result;
    }

    private Node addPeriodIdColumn(PeriodConvertorConfig config, Node result) {
        if (result.getSchema(InterfaceName.PeriodId.name()) != null) {
            result = result.discard(InterfaceName.PeriodId.name());
        }
        if (config.getPeriodStrategy() == null) {
            result = result.addColumnWithFixedValue(InterfaceName.PeriodId.name(), null, Integer.class);
        } else {
            result = result.apply(
                    new ConsolidateAddPeriodColumnFunction(config.getPeriodStrategy(), config.getTrxDateField(),
                            config.getPeriodField()),
                    new FieldList(config.getTrxDateField()),
                    new FieldMetadata(InterfaceName.PeriodId.name(), Integer.class));
        }
        return result;
    }

    private Node addCompositeIdColumn(Node node) {
        List<String> keys = Arrays.asList(InterfaceName.AccountId.name(), InterfaceName.PeriodId.name());
        List<String> fieldNames = node.getFieldNames();
        if (fieldNames.contains(COMPOSITE_KEY)) {
            return node;
        }
        node = node.apply(new ConsolidateAddCompositeColumnFuction(keys, COMPOSITE_KEY), new FieldList(keys),
                new FieldMetadata(COMPOSITE_KEY, String.class));
        return node;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodConvertorConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_CONVERTOR;

    }
}
