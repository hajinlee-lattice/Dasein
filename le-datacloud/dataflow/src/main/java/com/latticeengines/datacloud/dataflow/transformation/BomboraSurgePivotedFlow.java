package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.math.IntRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.BitEncodeUtils;
import com.latticeengines.datacloud.dataflow.utils.FileParser;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.BomboraSurgeIntentFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BomboraSurgeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(BomboraSurgePivotedFlow.DATAFLOW_BEAN_NAME)
public class BomboraSurgePivotedFlow extends ConfigurableFlowBase<BomboraSurgeConfig> {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(BomboraSurgePivotedFlow.class);

    public static final String DATAFLOW_BEAN_NAME = "bomboraSurgePivotedFlow";

    public static final String TRANSFORMER_NAME = "bomboraSurgePivotedTransformer";

    private static String[] groupByFieldsForEncode = new String[] { "Domain" };

    private static String[] groupByFields = new String[] { "Domain", "Topic" };

    private static final String INTENT = "Intent";

    private BomboraSurgeConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Map<String, Map<IntRange, String>> intentMap = FileParser.parseBomboraIntent();
        Node bomburaSurge = addSource(parameters.getBaseTables().get(0));
        bomburaSurge = bomburaSurge.groupByAndLimit(new FieldList(groupByFields), 1);
        bomburaSurge = addIntent(bomburaSurge, intentMap);

        List<SourceColumn> sourceColumns = parameters.getColumns();
        Node encoded = BitEncodeUtils.encode(bomburaSurge, groupByFieldsForEncode, sourceColumns);
        return encoded;
    }

    private Node addIntent(Node node, Map<String, Map<IntRange, String>> intentMap) {
        return node.apply(new BomboraSurgeIntentFunction(INTENT, config.getCompoScoreField(),
                config.getBucketCodeField(), intentMap), new FieldList(node.getFieldNames()),
                new FieldMetadata(INTENT, String.class));
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
        return BomboraSurgeConfig.class;
    }
}
