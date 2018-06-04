package com.latticeengines.serviceflows.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_REMOVED;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.SOURCE_PREFIX;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.ParseMatchResultParameters;

@Component("parseMatchResult")
public class ParseMatchResult extends TypesafeDataFlowBuilder<ParseMatchResultParameters> {

    private static final Logger log = LoggerFactory.getLogger(PrepareMatchDataflow.class);
    private List<String> sourceCols;

    @Override
    public Node construct(ParseMatchResultParameters parameters) {
        sourceCols = parameters.sourceColumns;
        Node matchNode = addSource(parameters.matchTableName);
        Node resultNode = resolveConflictingFields(matchNode);
        if (parameters.excludeDataCloudAttrs) {
            List<String> fieldFilter = new ArrayList<>(sourceCols);
            addExtraAttrs(fieldFilter, resultNode, parameters);
            List<String> fieldsToRetain = new ArrayList<>(resultNode.getFieldNames());
            fieldsToRetain.retainAll(fieldFilter);
            resultNode = resultNode.retain(new FieldList(fieldsToRetain));
        }

        if (StringUtils.isNotBlank(parameters.sourceTableName) && StringUtils.isNotBlank(parameters.idColumnName)) {
            resultNode = joinSourceTable(parameters, resultNode);
        }
        return resultNode;

    }

    private Node joinSourceTable(ParseMatchResultParameters parameters, Node resultNode) {
        Node sourceNode = addSource(parameters.sourceTableName);
        List<String> retainFields = new ArrayList<>(sourceNode.getFieldNames());
        Set<String> retainSet = new HashSet<>(retainFields);
        List<String> matchFields = resultNode.getFieldNames();
        matchFields.forEach(field -> {
            if (!retainSet.contains(field)) {
                retainFields.add(field);
                retainSet.add(field);
            }
        });
        String idColumnName = matchFields.contains(InterfaceName.InternalId.name()) ? InterfaceName.InternalId.name()
                : parameters.idColumnName;
        FieldList idColumn = new FieldList(idColumnName);
        resultNode = sourceNode.leftJoin(idColumn, resultNode, idColumn);
        resultNode = resultNode.retain(new FieldList(retainFields));
        return resultNode;
    }

    private Node resolveConflictingFields(Node node) {
        List<String> conflictingFields = findConflictingFields(node);
        log.warn("Found conflicting fields: " + StringUtils.join(conflictingFields, ", "));
        if (!conflictingFields.isEmpty()) {
            node = node.discard(new FieldList(conflictingFields));
            FieldList[] renameFieldLists = renameFields(conflictingFields);
            if (renameFieldLists[0].getFields().length > 0) {
                node = node.rename(renameFieldLists[0], renameFieldLists[1]);
                node = node.retain(new FieldList(node.getFieldNames()));
            }
        }
        return node;
    }

    private List<String> findConflictingFields(Node node) {
        List<FieldMetadata> fms = node.getSchema();
        Set<String> fieldsInAvro = new HashSet<>();
        for (FieldMetadata fm : fms) {
            fieldsInAvro.add(fm.getFieldName());
        }

        List<String> conflictingFields = new ArrayList<>();
        for (String sourceCol : sourceCols) {
            if (fieldsInAvro.contains(sourceCol) && fieldsInAvro.contains(SOURCE_PREFIX + sourceCol)) {
                conflictingFields.add(sourceCol);
            }
        }
        return conflictingFields;
    }

    private FieldList[] renameFields(List<String> conflictingFields) {
        FieldList fieldsWithOutPrefix = new FieldList(conflictingFields);
        String[] namesWithPrefix = new String[conflictingFields.size()];
        for (int i = 0; i < conflictingFields.size(); i++) {
            namesWithPrefix[i] = SOURCE_PREFIX + conflictingFields.get(i);
        }
        FieldList fieldsWithPrefix = new FieldList(namesWithPrefix);
        return new FieldList[] { fieldsWithPrefix, fieldsWithOutPrefix };
    }

    private void addExtraAttrs(List<String> fieldFilter, Node node, ParseMatchResultParameters parameters) {
        // Only modeling has these dedupe fields
        List<String> fieldNames = node.getFieldNames();
        if (parameters.keepLid && fieldNames.contains(InterfaceName.LatticeAccountId.name())) {
            fieldFilter.add(InterfaceName.LatticeAccountId.name());
        }
        if (fieldNames.contains(INT_LDC_DEDUPE_ID)) {
            fieldFilter.add(INT_LDC_LID);
            fieldFilter.add(INT_LDC_DEDUPE_ID);
            fieldFilter.add(INT_LDC_REMOVED);
        }
    }

}
