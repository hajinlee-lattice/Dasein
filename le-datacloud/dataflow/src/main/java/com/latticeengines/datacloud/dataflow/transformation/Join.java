package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.JoinConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;


@Component(Join.DATAFLOW_BEAN_NAME)
public class Join extends ConfigurableFlowBase<JoinConfig> {
    public static final String DATAFLOW_BEAN_NAME = "JoinFlow";
    public static final String TRANSFORMER_NAME = "JoinTransformer";

    private JoinConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        if (config.getJoinFields() == null || config.getJoinFields().length != parameters.getBaseTables().size()) {
            throw new RuntimeException("Every base source should have join fields");
        }
        if (config.getJoinType() == null) {
            throw new RuntimeException("Join type is missing");
        }
        Node src = addSource(parameters.getBaseTables().get(0));
        if (parameters.getBaseTables().size() == 1) {
            return src;
        }
        List<Node> joinedSrcs = new ArrayList<>();
        for (int i = 1; i < parameters.getBaseTables().size(); i++) {
            joinedSrcs.add(addSource(parameters.getBaseTables().get(i)));
        }
        List<FieldList> joinedFields = new ArrayList<>();
        int joinedFieldsNum = -1;
        for (int i = 1; i < config.getJoinFields().length; i++) {
            String[] fields = config.getJoinFields()[i];
            if (fields == null || fields.length == 0) {
                throw new RuntimeException("Join field must be at least one");
            }
            if (joinedFieldsNum == -1) {
                joinedFieldsNum = fields.length;
            } else if (fields.length != joinedFieldsNum) {
                throw new RuntimeException("Number of join fields should be same for each base source");
            }
            joinedFields.add(new FieldList(fields));
        }

        JoinType type = null;
        switch (config.getJoinType()) {
        case INNER:
            type = JoinType.INNER;
            break;
        case OUTER:
            type = JoinType.OUTER;
            break;
        case LEFT:
            type = JoinType.LEFT;
            break;
        case RIGHT:
            type = JoinType.RIGHT;
            break;
        default:
            throw new RuntimeException("Not supported join type " + config.getJoinType());
        }
        Node joined = src.coGroup(new FieldList(config.getJoinFields()[0]), joinedSrcs, joinedFields, type);
        return joined;
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
        return JoinConfig.class;
    }
}
