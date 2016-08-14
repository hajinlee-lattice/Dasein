package com.latticeengines.serviceflows.dataflow.match;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private static final Log log = LogFactory.getLog(CascadingBulkMatchDataflow.class);

    private String domainIndexFieldName = MatchKey.Domain.name();
    private String dunsIndexFieldName = MatchKey.DUNS.name();
    private String latticeIdFieldName = "LatticeID";

    @Override
    public Node construct(CascadingBulkMatchDataflowParameters parameters) {
        FieldList latticeIdField = new FieldList(latticeIdFieldName);

        Node inputSource = addSource(parameters.getInputAvro());
        Node accountMasterIndexSource = addSource(parameters.getAccountMasterIndex());
        Node matchedNode = matchDomainField(parameters, inputSource, accountMasterIndexSource, latticeIdField);
        matchedNode = matchDunsField(parameters, inputSource, accountMasterIndexSource, matchedNode, latticeIdField);

        Node accountMasterSource = addSource(parameters.getAccountMaster());
        matchedNode = matchedNode.join(latticeIdField, accountMasterSource, latticeIdField, JoinType.INNER);

        FieldList fieldList = buildOutputFieldList(parameters);
        log.info("output fields=" + fieldList.getFieldsAsList());
        matchedNode = matchedNode.retain(fieldList);

        return matchedNode;
    }

    private FieldList buildOutputFieldList(CascadingBulkMatchDataflowParameters parameters) {
        FieldList fieldList = buildFieldListFromSchema(parameters.getOutputSchemaPath());
        Map<MatchKey, List<String>> keyMap = parameters.getKeyMap();
        if (keyMap != null) {
            List<String> newFields = new ArrayList<>();
            if (keyMap.containsKey(MatchKey.Domain) && CollectionUtils.isNotEmpty(keyMap.get(MatchKey.Domain))) {
                newFields.add(domainIndexFieldName);
            }
            if (keyMap.containsKey(MatchKey.DUNS) && CollectionUtils.isNotEmpty(keyMap.get(MatchKey.DUNS))) {
                newFields.add(dunsIndexFieldName);
            }
            fieldList = fieldList.addAll(newFields);
        }
        return fieldList;
    }

    private Node matchDomainField(CascadingBulkMatchDataflowParameters parameters, Node inputSource,
            Node accountMasterIndexSource, FieldList latticeIdField) {
        Map<MatchKey, List<String>> keyMap = parameters.getKeyMap();
        Node resultNode = null;
        if (keyMap == null || !keyMap.containsKey(MatchKey.Domain)
                || CollectionUtils.isEmpty(keyMap.get(MatchKey.Domain))) {
            return resultNode;
        }
        List<String> domainNames = keyMap.get(MatchKey.Domain);
        FieldList domainIndexField = new FieldList(domainIndexFieldName);
        for (String inputDomainFieldName : domainNames) {
            FieldList inputDomainField = new FieldList(inputDomainFieldName);
            inputSource = inputSource.apply(new DomainCleanupFunction(inputDomainFieldName), inputDomainField,
                    new FieldMetadata(inputDomainFieldName, String.class));
            Node matchedNode = inputSource.join(inputDomainField, accountMasterIndexSource, domainIndexField,
                    JoinType.INNER);
            matchedNode = matchedNode.retain(latticeIdField);
            if (resultNode == null) {
                resultNode = matchedNode;
            } else {
                resultNode = resultNode.merge(matchedNode);
            }
        }
        if (resultNode != null) {
            resultNode = resultNode.groupByAndLimit(latticeIdField, 1);
        }
        return resultNode;
    }

    private Node matchDunsField(CascadingBulkMatchDataflowParameters parameters, Node inputSource,
            Node accountMasterIndexSource, Node matchedNode, FieldList latticeIdField) {
        Map<MatchKey, List<String>> keyMap = parameters.getKeyMap();
        if (keyMap == null || !keyMap.containsKey(MatchKey.DUNS) || CollectionUtils.isEmpty(keyMap.get(MatchKey.DUNS))) {
            return matchedNode;
        }
        String inputDunsFieldName = keyMap.get(MatchKey.DUNS).get(0);
        FieldList inputDunsField = new FieldList(inputDunsFieldName);
        FieldList dunsIndexField = new FieldList(dunsIndexFieldName);
        Node matchedDunsNode = inputSource.join(inputDunsField, accountMasterIndexSource, dunsIndexField,
                JoinType.INNER);
        matchedDunsNode = matchedDunsNode.retain(latticeIdField);
        if (matchedNode != null) {
            matchedNode = matchedNode.merge(matchedDunsNode);
            matchedNode = matchedNode.groupByAndLimit(latticeIdField, 1);
            return matchedNode;
        } else {
            return matchedDunsNode;
        }

    }
}
