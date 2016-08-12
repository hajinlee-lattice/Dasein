package com.latticeengines.serviceflows.dataflow.match;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;

@Component("cascadingBulkMatchDataflow")
public class CascadingBulkMatchDataflow extends TypesafeDataFlowBuilder<CascadingBulkMatchDataflowParameters> {

    private String domainFieldName = MatchKey.Domain.name();
    private String dunsFieldName = MatchKey.DUNS.name();
    private String latticeIdFieldName = MatchKey.LatticeAccountID.name();

    @Override
    public Node construct(CascadingBulkMatchDataflowParameters parameters) {
        FieldList latticeIdField = new FieldList(latticeIdFieldName);

        Node source = cleanSource(parameters);
        Node matchedNode = matchDomainIndex(parameters, source, latticeIdField);
        matchedNode = matchDunsIndex(parameters, source, matchedNode, latticeIdField);

        Node accountMasterSource = addSource(parameters.getAccountMaster());
        matchedNode = matchedNode.join(latticeIdField, accountMasterSource, latticeIdField, JoinType.INNER);
        matchedNode = matchedNode.retain(buildFieldListFromSchema(parameters.getOutputSchemaPath()));

        return matchedNode;
    }

    private Node cleanSource(CascadingBulkMatchDataflowParameters parameters) {
        if (parameters.getKeyMap() != null && parameters.getKeyMap().containsKey(MatchKey.Domain)) {
            domainFieldName = parameters.getKeyMap().get(MatchKey.Domain).get(0);
        }
        Node source = addSource(parameters.getInputAvro());
        source = source.apply(new DomainCleanupFunction(domainFieldName), new FieldList(domainFieldName),
                new FieldMetadata(domainFieldName, String.class));
        return source;
    }

    private Node matchDomainIndex(CascadingBulkMatchDataflowParameters parameters, Node source, FieldList latticeIdField) {
        FieldList domainJoinFields = new FieldList(domainFieldName);
        Node domainIndexSource = addSource(parameters.getDomainIndex());
        Node matchedNode = source.join(domainJoinFields, domainIndexSource, domainJoinFields, JoinType.INNER);
        matchedNode = matchedNode.retain(latticeIdField);
        return matchedNode;
    }

    private Node matchDunsIndex(CascadingBulkMatchDataflowParameters parameters, Node source, Node matchedNode,
            FieldList latticeIdField) {
        if (parameters.getKeyMap() != null && parameters.getKeyMap().containsKey(MatchKey.DUNS)) {
            dunsFieldName = parameters.getKeyMap().get(MatchKey.DUNS).get(0);
        }
        Node dunsIndexSource = null;
        if (StringUtils.isNotBlank(parameters.getDunsIndex())) {
            dunsIndexSource = addSource(parameters.getDunsIndex());
        }
        if (dunsIndexSource != null) {
            FieldList dunsField = new FieldList(dunsFieldName);
            Node matchedDunsNode = source.join(dunsField, dunsIndexSource, dunsField, JoinType.INNER);
            matchedDunsNode = matchedDunsNode.retain(latticeIdField);
            matchedNode = matchedNode.merge(matchedDunsNode);
            matchedNode = matchedNode.groupByAndLimit(latticeIdField, 1);
        }
        return matchedNode;
    }

}
