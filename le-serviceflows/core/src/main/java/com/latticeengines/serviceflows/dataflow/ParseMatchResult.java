package com.latticeengines.serviceflows.dataflow;

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
import com.latticeengines.domain.exposed.datacloud.match.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("parseMatchResult")
public class ParseMatchResult extends TypesafeDataFlowBuilder<ParseMatchResultParameters> {

    private static final Log log = LogFactory.getLog(ParseMatchResult.class);

    private static final String SOURCE_PREFIX = "Source_";
    private List<String> sourceCols;

    @Override
    public Node construct(ParseMatchResultParameters parameters) {
        sourceCols = parameters.sourceColumns;
        Node source = addSource(parameters.sourceTableName);

        List<String> conflictingFields = findConflictingFields(source);
        log.info("Found conflicting fields: " + StringUtils.join(conflictingFields, ", "));
        if (!conflictingFields.isEmpty()) {
            FieldList retainFields = retainFields(source, conflictingFields);
            source = source.retain(retainFields);
            FieldList[] renameFieldLists = renameFields(conflictingFields);
            source = source.rename(renameFieldLists[0], renameFieldLists[1]);
        }

        return source;
    }

    private List<String> findConflictingFields(Node node) {
        List<FieldMetadata> fms = node.getSchema();
        Set<String> fieldsInAvro = new HashSet<>();
        for (FieldMetadata fm : fms) {
            fieldsInAvro.add(fm.getFieldName());
        }

        List<String> conflictingFields = new ArrayList<>();
        for (String sourceCol: sourceCols) {
            if (fieldsInAvro.contains(sourceCol) && fieldsInAvro.contains(SOURCE_PREFIX + sourceCol)) {
                conflictingFields.add(sourceCol);
            }
        }
        return  conflictingFields;
    }

    private FieldList retainFields(Node node, List<String> conflictingFields) {
        List<FieldMetadata> fms = node.getSchema();
        List<String> retainFieldNames = new ArrayList<>();
        for (FieldMetadata fm : fms) {
            if (!conflictingFields.contains(fm.getFieldName())) {
                retainFieldNames.add(fm.getFieldName());
            }
        }
        return new FieldList(retainFieldNames.toArray(new String[retainFieldNames.size()]));
    }

    private FieldList[] renameFields(List<String> conflictingFields) {
        FieldList fieldsWithOutPrefix = new FieldList(conflictingFields.toArray(new String[conflictingFields.size()]));
        String[] namesWithPrefix = new String [conflictingFields.size()];
        for (int i = 0; i < conflictingFields.size(); i++) {
            namesWithPrefix[i] = SOURCE_PREFIX + conflictingFields.get(i);
        }
        FieldList fieldsWithPrefix = new FieldList(namesWithPrefix);
        return new FieldList[]{ fieldsWithPrefix, fieldsWithOutPrefix };
    }

}
