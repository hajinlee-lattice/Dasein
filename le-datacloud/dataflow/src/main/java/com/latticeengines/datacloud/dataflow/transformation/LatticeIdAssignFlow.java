package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdRefreshFlowParameter;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;

@Component("latticeIdAssignFlow")
public class LatticeIdAssignFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, LatticeIdRefreshFlowParameter> {
    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(LatticeIdAssignFlow.class);

    private final static String ID = "ID_";

    @Override
    public Node construct(LatticeIdRefreshFlowParameter parameters) {
        LatticeIdStrategy strategy = parameters.getStrategy();
        List<String> idsKeys = getIdsKeys(strategy);
        List<String> entityKeys = getEntityKeys(strategy);

        Node ids = addSource(parameters.getBaseTables().get(0));
        Node entity = addSource(parameters.getBaseTables().get(1));
        ids = renameIds(ids);
        entity = dropIdIfExist(entity, strategy);

        List<String> finalFields = new ArrayList<>();
        finalFields.addAll(entity.getFieldNames());
        finalFields.add(strategy.getIdName());

        Node joined = entity.join(new FieldList(entityKeys), ids, new FieldList(idsKeys), JoinType.INNER);
        joined = joined.rename(new FieldList(ID + strategy.getIdName()), new FieldList(strategy.getIdName()));
        joined = joined.retain(new FieldList(finalFields));

        return joined;
    }

    private Node dropIdIfExist(Node entity, LatticeIdStrategy strategy) {
        boolean flag = false;
        for (String field : entity.getFieldNames()) {
            if (field.equals(strategy.getIdName())) {
                flag = true;
                break;
            }
        }
        if (flag) {
            entity = entity.discard(new FieldList(strategy.getIdName()));
        }
        return entity;
    }

    private Node renameIds(Node src) {
        for (String attr : src.getFieldNames()) {
            src = src.rename(new FieldList(attr), new FieldList(ID + attr));
        }
        return src;
    }

    private List<String> getIdsKeys(LatticeIdStrategy strategy) {
        List<String> fields = new ArrayList<>();
        for (List<String> attrs : strategy.getKeyMap().values()) {
            for (String attr : attrs) {
                fields.add(ID + attr);
            }
        }
        return fields;
    }

    private List<String> getEntityKeys(LatticeIdStrategy strategy) {
        List<String> fields = new ArrayList<>();
        for (List<String> attrs : strategy.getKeyMap().values()) {
            fields.addAll(attrs);
        }
        return fields;
    }

}
