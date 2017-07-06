package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMSeedPriLocAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AMSeedPriActConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;

@Component(AMSeedPriActFix.DATAFLOW_BEAN_NAME)
public class AMSeedPriActFix extends ConfigurableFlowBase<AMSeedPriActConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMSeedPriActFix";
    public static final String TRANSFORMER_NAME = "AMSeedPriActFixTransformer";

    private AMSeedPriActConfig config;

    private static final String LATTICEID_PRILOC = "_LatticeID_PriLoc_";

    @SuppressWarnings("rawtypes")
    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node ams = addSource(parameters.getBaseTables().get(0));
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(LATTICEID_PRILOC, Long.class));
        Aggregator agg = new AMSeedPriLocAggregator(new Fields(LATTICEID_PRILOC), config.getLatticeIdField(),
                config.getDomainField(), config.getDunsField(), config.getDuDunsField(), config.getGuDunsField(),
                config.getEmployeeField(), config.getSalesVolUSField(), config.getIsPriLocField(),
                config.getCountryField(), config.getIsPriActField());
        Node priLoc = ams.groupByAndAggregate(new FieldList(config.getDomainField()), agg, fms)
                .renamePipe("PrimaryLocation");
        ams = ams.join(new FieldList(config.getLatticeIdField()), priLoc, new FieldList(LATTICEID_PRILOC),
                JoinType.LEFT);
        ams = ams.discard(new FieldList(config.getIsPriLocField()))
                .apply(String.format("(%s == null || %s != null) ? \"Y\" : \"N\"", config.getDomainField(),
                        LATTICEID_PRILOC), new FieldList(config.getDomainField(), LATTICEID_PRILOC),
                new FieldMetadata(config.getIsPriLocField(), String.class)).discard(new FieldList(LATTICEID_PRILOC));
        return ams;
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
        return AMSeedPriActConfig.class;
    }

}
