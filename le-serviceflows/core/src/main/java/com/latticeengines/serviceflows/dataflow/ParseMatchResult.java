package com.latticeengines.serviceflows.dataflow;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.match.ParseMatchResultParameters;

@Component("parseMatchResult")
public class ParseMatchResult extends TypesafeDataFlowBuilder<ParseMatchResultParameters> {

    private static final String SOURCE_PREFIX = "Source_";
    private List<String> sourceCols;

    @Override
    public Node construct(ParseMatchResultParameters parameters) {
        sourceCols = parameters.sourceColumns;
        Node source = addSource(parameters.sourceTableName);
        FieldList[] fields = sourceFields(source);
        FieldList retainFields = retainFields(source);
        source = source.retain(retainFields);
        return source.rename(fields[0], fields[1]);
    }

    private FieldList[] sourceFields(Node source) {
        List<FieldMetadata> fms = source.getSchema();
        List<FieldMetadata> sourceFields = new ArrayList<>();
        for (FieldMetadata fm : fms) {
            if (fm.getFieldName().startsWith(SOURCE_PREFIX)
                    && sourceCols.contains(fm.getFieldName().substring(SOURCE_PREFIX.length()))) {
                sourceFields.add(fm);
            }
        }

        String[] originalFields = new String[sourceFields.size()];
        String[] newFields = new String[sourceFields.size()];

        for (int i = 0; i < sourceFields.size(); i++) {
            String fieldName = sourceFields.get(i).getFieldName();
            String newFieldName = fieldName.substring(SOURCE_PREFIX.length());
            if (sourceCols.contains(newFieldName)) {
                originalFields[i] = fieldName;
                newFields[i] = fieldName.substring(SOURCE_PREFIX.length());
                // Don't remove the Source prefix if there's a name collision.
                if (source.getFieldNames().contains(newFields[i])) {
                    newFields[i] = fieldName;
                }
            }
        }
        return new FieldList[] { new FieldList(originalFields), new FieldList(newFields) };
    }

    private FieldList retainFields(Node source) {
        List<FieldMetadata> fms = source.getSchema();
        List<String> retainFieldNames = new ArrayList<>();
        for (FieldMetadata fm : fms) {
            if (!fm.getFieldName().startsWith(SOURCE_PREFIX)
                    || sourceCols.contains(fm.getFieldName().substring(SOURCE_PREFIX.length()))) {
                retainFieldNames.add(fm.getFieldName());
            }
        }
        return new FieldList(retainFieldNames.toArray(new String[retainFieldNames.size()]));
    }

}
