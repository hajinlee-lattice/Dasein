package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.LatticeIdAssignFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("latticeIdAssignFlow")
public class LatticeIdAssignFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, LatticeIdAssignFlowParameter> {
    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(LatticeIdAssignFlow.class);

    @Override
    public Node construct(LatticeIdAssignFlowParameter parameters) {
        Node ams = addSource(parameters.getBaseTables().get(0));
        Node amId = addSource(parameters.getBaseTables().get(1));

        List<String> finalFields = new ArrayList<String>();
        finalFields.addAll(ams.getFieldNames());
        finalFields.add(parameters.getAmSeedIdField());
        ams = ams.join(new FieldList(parameters.getAmSeedDunsField(), parameters.getAmSeedDomainField()), amId,
                new FieldList(parameters.getAmIdSrcDunsField(), parameters.getAmIdSrcDomainField()), JoinType.LEFT);
        ams = ams.rename(new FieldList(parameters.getAmIdSrcIdField()), new FieldList(parameters.getAmSeedIdField()))
                .retain(new FieldList(finalFields));

        Node hasId = ams.filter(String.format("%s != null", parameters.getAmSeedIdField()),
                new FieldList(parameters.getAmSeedIdField()));
        Node noId = ams.filter(String.format("%s == null", parameters.getAmSeedIdField()),
                new FieldList(parameters.getAmSeedIdField())).discard(parameters.getAmSeedIdField());

        noId = noId.addRowID(parameters.getAmSeedIdField());
        noId = noId.apply(
                String.format("Long.valueOf(%s) + %d", parameters.getAmSeedIdField(), parameters.getCurrentCount()),
                new FieldList(parameters.getAmSeedIdField()),
                new FieldMetadata(parameters.getAmSeedIdField(), Long.class));
        return hasId.merge(noId);
    }
}
