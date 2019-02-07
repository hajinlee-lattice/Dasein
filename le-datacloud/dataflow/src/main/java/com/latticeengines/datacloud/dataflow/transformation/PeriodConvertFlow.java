package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
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

import cascading.tuple.Fields;

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
        if (CollectionUtils.isEmpty(config.getPeriodStrategies())) {
            result = result.addColumnWithFixedValue(InterfaceName.PeriodId.name(), null, Integer.class);
        } else {
            if (result.getSchema(InterfaceName.PeriodName.name()) != null) {
                result = result.discard(InterfaceName.PeriodName.name());
            }
            List<FieldMetadata> fms = new ArrayList<>();
            fms.add(new FieldMetadata(InterfaceName.PeriodName.name(), String.class));
            fms.add(new FieldMetadata(InterfaceName.PeriodId.name(), Integer.class));
            List<String> outputFields = result.getFieldNames();
            outputFields.add(InterfaceName.PeriodName.name());
            outputFields.add(InterfaceName.PeriodId.name());

            result = result.apply(
                    new ConsolidateAddPeriodColumnFunction(
                            new Fields(InterfaceName.PeriodName.name(), InterfaceName.PeriodId.name()),
                            config.getPeriodStrategies(), config.getTrxDateField(),
                            InterfaceName.PeriodName.name(), InterfaceName.PeriodId.name()),
                    new FieldList(result.getFieldNames()), fms, new FieldList(outputFields));
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
