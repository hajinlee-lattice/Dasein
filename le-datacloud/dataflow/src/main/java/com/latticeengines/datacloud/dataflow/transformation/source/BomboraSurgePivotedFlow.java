package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.Range;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.datacloud.dataflow.utils.BitEncodeUtils;
import com.latticeengines.datacloud.dataflow.utils.FileParser;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.source.BomboraSurgeIntentFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.BomboraSurgeConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

/**
 * A pipeline step of BomboraSurge pipeline
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-BomboraSurgeCreation
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-BomboraSurgePivotedCreation
 */
@Component(BomboraSurgePivotedFlow.DATAFLOW_BEAN_NAME)
public class BomboraSurgePivotedFlow extends ConfigurableFlowBase<BomboraSurgeConfig> {

    public static final String DATAFLOW_BEAN_NAME = "bomboraSurgePivotedFlow";

    public static final String TRANSFORMER_NAME = "bomboraSurgePivotedTransformer";

    private static String[] groupByFieldsForEncode = new String[] { "Domain" };

    @SuppressWarnings("unused")
    private static String[] groupByFields = new String[] { "Domain", "Topic" };

    private static final String INTENT = "Intent";

    private BomboraSurgeConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Map<Range<Integer>, String> intentMap = FileParser.parseBomboraIntent();
        Node bomboraSurge = addSource(parameters.getBaseTables().get(0));
        bomboraSurge = addIntent(bomboraSurge, intentMap);

        List<SourceColumn> sourceColumns = parameters.getColumns();
        Node encoded = BitEncodeUtils.encode(bomboraSurge, groupByFieldsForEncode, sourceColumns);
        return encoded;
    }

    private Node addIntent(Node node, Map<Range<Integer>, String> intentMap) {
        return node.apply(new BomboraSurgeIntentFunction(INTENT, config.getCompoScoreField(), intentMap),
                new FieldList(node.getFieldNames()), new FieldMetadata(INTENT, String.class));
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
