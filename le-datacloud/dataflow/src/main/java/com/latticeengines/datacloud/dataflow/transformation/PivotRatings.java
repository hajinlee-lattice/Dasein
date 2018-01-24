package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.PivotRatings.BEAN_NAME;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.PivotRatingsConfig;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component(BEAN_NAME)
public class PivotRatings extends ConsolidateBaseFlow<PivotRatingsConfig> {

    public static final String BEAN_NAME = "pivotRatings";
    public static final String TRANSFORMER_NAME = DataCloudConstants.TRANSFORMER_PIVOT_RATINGS;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PivotRatingsConfig config = getTransformerConfig(parameters);
        Map<String, String> idAttrsMap = config.getIdAttrsMap();
        Node rawRatings = addSource(parameters.getBaseTables().get(0));
        Node pivoted = pivotRuleBased(rawRatings, idAttrsMap);
        return renameIds(pivoted, idAttrsMap);
    }

    private Node pivotRuleBased(Node rawRatings, Map<String, String> idAttrsMap) {
        String keyCol = InterfaceName.ModelId.name();
        String valCol = InterfaceName.Rating.name();
        Set<String> pivotedKeys = idAttrsMap.keySet();
        PivotStrategyImpl pivotStrategy = PivotStrategyImpl.max(keyCol, valCol, pivotedKeys, String.class, null);
        return rawRatings.pivot(new String[]{InterfaceName.AccountId.name()}, pivotStrategy);
    }

    private Node renameIds(Node node, Map<String, String> idAttrsMap) {
        List<String> modelIdAttrNames = new ArrayList<>();
        List<String> engineIdAttrNames = new ArrayList<>();
        for (Map.Entry<String, String> entry: idAttrsMap.entrySet()) {
            modelIdAttrNames.add(entry.getKey());
            engineIdAttrNames.add(entry.getValue());
        }
        return node.rename(new FieldList(modelIdAttrNames), new FieldList(engineIdAttrNames));
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PivotRatingsConfig.class;
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
