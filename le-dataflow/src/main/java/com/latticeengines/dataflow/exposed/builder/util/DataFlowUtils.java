package com.latticeengines.dataflow.exposed.builder.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;

import cascading.operation.Aggregator;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.tuple.Fields;

import com.google.common.base.Joiner;
import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.FieldMetadata;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class DataFlowUtils {
    public static List<FieldMetadata> getIntersection(List<String> partial, List<FieldMetadata> full) {
        Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(full);
        List<FieldMetadata> partialFieldMetadata = new ArrayList<>();

        for (String fieldName : partial) {
            FieldMetadata fm = nameToFieldMetadataMap.get(fieldName);

            if (fm == null) {
                throw new LedpException(LedpCode.LEDP_26002, new String[] { fieldName });
            }
            partialFieldMetadata.add(fm);
        }
        return partialFieldMetadata;
    }

    public static Map<String, FieldMetadata> getFieldMetadataMap(List<FieldMetadata> fieldMetadata) {
        Map<String, FieldMetadata> nameToFieldMetadataMap = new HashMap<>();

        for (FieldMetadata fieldMetadatum : fieldMetadata) {
            nameToFieldMetadataMap.put(fieldMetadatum.getFieldName(), fieldMetadatum);
        }
        return nameToFieldMetadataMap;
    }

    public static List<String> getFieldNames(List<FieldMetadata> fieldMetadata) {
        List<String> fieldNames = new ArrayList<>();
        for (FieldMetadata fieldMetadatum : fieldMetadata) {
            fieldNames.add(fieldMetadatum.getFieldName());
        }
        return fieldNames;
    }

    public static Fields convertToFields(List<String> fields) {
        String[] fieldsArray = new String[fields.size()];
        fields.toArray(fieldsArray);
        return new Fields(fieldsArray);
    }

    public static Fields convertToFields(String[] fields) {
        return new Fields(fields);
    }

    public static Fields convertToFields(String field) {
        return new Fields(field);
    }

    public static Aggregator<?> getAggregator(String aggregatedFieldName, AggregationType aggregationType) {
        switch (aggregationType) {
        case MAX:
            return new MaxValue(convertToFields(aggregatedFieldName));
        case MIN:
            return new MinValue(convertToFields(aggregatedFieldName));
        case COUNT:
            return new Count(convertToFields(aggregatedFieldName));
        case SUM:
            return new Sum(convertToFields(aggregatedFieldName));
        case AVG:
            return new Average(convertToFields(aggregatedFieldName));
        case FIRST:
            return new First();
        case LAST:
            return new Last();
        }
        return null;
    }

    public static Class<?>[] getTypes(List<String> fieldNames, List<FieldMetadata> full) {
        List<FieldMetadata> fmList = getIntersection(fieldNames, full);

        Class<?>[] types = new Class[fmList.size()];

        int i = 0;
        for (FieldMetadata fm : fmList) {
            types[i++] = fm.getJavaType();
        }
        return types;
    }

    public static List<FieldMetadata> retainFields(FieldList outputFields, List<FieldMetadata> fm) {
        if (outputFields != null) {
            List<FieldMetadata> newFieldMetadata = new ArrayList<>();
            Map<String, FieldMetadata> nameToFieldMetadataMap = getFieldMetadataMap(fm);
            Set<String> metadataKeySet = nameToFieldMetadataMap.keySet();
            List<String> outputFieldList = outputFields.getFieldsAsList();
            for (String outputField : outputFieldList) {
                if (metadataKeySet.contains(outputField)) {
                    newFieldMetadata.add(nameToFieldMetadataMap.get(outputField));
                }
            }
            return newFieldMetadata;
        }
        return fm;
    }

    public static String createRegexFromGlob(String glob) {
        String out = "^";
        for (int i = 0; i < glob.length(); ++i) {
            final char c = glob.charAt(i);
            switch (c) {
            case '*':
                out += ".*";
                break;
            case '?':
                out += '.';
                break;
            case '.':
                out += "\\.";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                out += c;
            }
        }
        out += '$';
        return out;
    }

    public static String getLocationPrefixedPath(CascadingDataFlowBuilder cascadingDataFlowBuilder, String path) {
        DataFlowContext context = cascadingDataFlowBuilder.getDataFlowCtx();
        Configuration configuration = context.getRequiredProperty("HADOOPCONF", Configuration.class);

        if (path.startsWith("file://")) {
            path = path.substring(7);
        } else if (path.startsWith("hdfs://")) {
            String[] parts = path.split("/");
            // skip over hdfs://hostname:port/
            List<String> partsSkipped = new ArrayList<>();
            for (int i = 3; i < parts.length; ++i) {
                partsSkipped.add(parts[i]);
            }
            path = Joiner.on("/").join(partsSkipped);
        }

        return configuration.get("fs.defaultFS") + path;
    }
}
