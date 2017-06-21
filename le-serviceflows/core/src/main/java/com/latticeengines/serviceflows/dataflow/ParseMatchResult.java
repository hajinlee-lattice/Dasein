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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.serviceflows.core.dataflow.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("parseMatchResult")
public class ParseMatchResult extends TypesafeDataFlowBuilder<ParseMatchResultParameters> {

    private static final Log log = LogFactory.getLog(ParseMatchResult.class);
    private List<String> sourceCols;

    @Override
    public Node construct(ParseMatchResultParameters parameters) {
        sourceCols = parameters.sourceColumns;
        Node source = addSource(parameters.sourceTableName);

        source = resolveConflictingFields(source);

        if (parameters.excludeDataCloudAttrs) {
            List<String> fieldFilter = new ArrayList<>(sourceCols);
            addDedupeAttrs(fieldFilter, source);
            List<String> fieldsToRetain = new ArrayList<>(source.getFieldNames());
            fieldsToRetain.retainAll(fieldFilter);
            source = source.retain(new FieldList(fieldsToRetain));
        }

        return source;
    }

    private Node resolveConflictingFields(Node node) {
        List<String> conflictingFields = findConflictingFields(node);
        log.warn("Found conflicting fields: " + StringUtils.join(conflictingFields, ", "));
        if (!conflictingFields.isEmpty()) {
            node = node.discard(new FieldList(conflictingFields));
            FieldList[] renameFieldLists = renameFields(conflictingFields);
            if (renameFieldLists[0].getFields().length > 0) {
                node = node.rename(renameFieldLists[0], renameFieldLists[1]);
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

    private void addDedupeAttrs(List<String> fieldFilter, Node node) {
        // Only modeling has these dedupe fields
        List<String> fieldNames = node.getFieldNames();
        if (fieldNames.contains(INT_LDC_DEDUPE_ID)) {
            fieldFilter.add(INT_LDC_LID);
            fieldFilter.add(INT_LDC_DEDUPE_ID);
            fieldFilter.add(INT_LDC_REMOVED);
        }
    }

}
