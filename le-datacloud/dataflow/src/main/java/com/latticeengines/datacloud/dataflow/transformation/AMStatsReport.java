package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.AMStatsReport.BEAN_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_ALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(BEAN_NAME)
public class AMStatsReport extends ConfigurableFlowBase<TransformerConfig> {

    public static final String BEAN_NAME = "AMStatsReport";
    public static final String TRANSFORMER_NAME = "AMStatsReport";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node stats = addSource(parameters.getBaseTables().get(0));
        stats = stats.filter(String.format("\"%s\".equals(%s)", InterfaceName.LatticeAccountId.name(), STATS_ATTR_NAME), new FieldList(STATS_ATTR_NAME));
        stats = stats.discard(STATS_ATTR_NAME, STATS_ATTR_BKTS, STATS_ATTR_ALGO);
        stats = stats.rename(new FieldList(STATS_ATTR_COUNT), new FieldList("RecordCount"));
        return stats;
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
