package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.AMStatsHQDunsJoin.BEAN_NAME;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

/**
 * Join AM with AMBucketed, to get HQDuns.
 */
@Component(BEAN_NAME)
public class AMStatsHQDunsJoin extends ConfigurableFlowBase<TransformerConfig> {

    public static final String BEAN_NAME = "amStatsHQDunsJoin";
    public static final String TRANSFORMER_NAME = "amStatsHQDunsJoiner";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        String latticeAccountId = InterfaceName.LatticeAccountId.name();
        Node amWithHQDuns = addSource(parameters.getBaseTables().get(0));
        Node amBkt = addSource(parameters.getBaseTables().get(1)).renamePipe("ambkt");
        amBkt = amBkt.leftJoin(new FieldList(latticeAccountId), amWithHQDuns, new FieldList(latticeAccountId));
        return amBkt;
    }

    @Override
    public Class<TransformerConfig> getTransformerConfigClass() {
        return TransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

}
