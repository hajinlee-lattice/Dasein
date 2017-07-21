package com.latticeengines.datacloud.dataflow.transformation;

import static com.latticeengines.datacloud.dataflow.transformation.ExtractCube.BEAN_NAME;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_ALGO;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_BKTS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_COUNT;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.STATS_ATTR_NAME;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.StatsRollupAggregator;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;


/**
 * This dataflow extract one cube from a stats avro with dimensions.
 * The cube is specified by a string->string map defining the dimenion values.
 * Default is the top cube.
 */
@Component(BEAN_NAME)
public class ExtractCube extends ConfigurableFlowBase<TransformerConfig> {

    public static final String BEAN_NAME = "extractCube";
    public static final String TRANSFORMER_NAME = "CubeExtractor";

    private static final String ALL = StatsRollupAggregator.ALL;

    private static final String[] STATS_ATTRS = new String[]{
            STATS_ATTR_NAME,
            STATS_ATTR_COUNT,
            STATS_ATTR_BKTS,
            STATS_ATTR_ALGO
    };

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node allCubes = addSource(parameters.getBaseTables().get(0));
        Map<String, String> cubeDef = getDimensions(allCubes.getFieldNames());
        Node cube = filter(allCubes, cubeDef);
        return cube.retain(new FieldList(STATS_ATTRS));
    }

    // (dim -> __ALL__)
    private Map<String, String> getDimensions(List<String> fieldNames) {
        Set<String> dims = new HashSet<>();
        Set<String> statsAttrs = new HashSet<>(Arrays.asList(STATS_ATTRS));
        fieldNames.forEach(f -> {
            if (!statsAttrs.contains(f)) {
                dims.add(f);
            }
        });
        Map<String, String> map = new HashMap<>();
        dims.forEach(d -> map.put(d, ALL));
        return map;
    }

    private Node filter(Node allCubes, Map<String, String> cubeDef) {
        List<String> equalClauses = new ArrayList<>();
        cubeDef.forEach((dim, val) -> equalClauses.add(String.format("\"%s\".equals(%s)", val, dim)));
        String expression = StringUtils.join(equalClauses, " && ");
        return allCubes.filter(expression, new FieldList(new ArrayList<>(cubeDef.keySet())));
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
