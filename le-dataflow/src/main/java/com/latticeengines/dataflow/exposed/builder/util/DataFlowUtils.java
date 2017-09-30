package com.latticeengines.dataflow.exposed.builder.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;

import com.google.common.base.Joiner;
import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

import cascading.operation.Aggregator;
import cascading.operation.aggregator.Average;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Last;
import cascading.operation.aggregator.MaxValue;
import cascading.operation.aggregator.MinValue;
import cascading.operation.aggregator.Sum;
import cascading.tuple.Fields;

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
        return fieldMetadata.stream().collect(Collectors.toMap(FieldMetadata::getFieldName, fm -> fm));
    }

    public static List<String> getFieldNames(List<FieldMetadata> fieldMetadata) {
        return fieldMetadata.stream().map(FieldMetadata::getFieldName).collect(Collectors.toList());
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

    public static List<FieldMetadata> combineFields(String pipeName, List<FieldMetadata> lhsFields,
            List<FieldMetadata> rhsFields) {
        List<FieldMetadata> declaredFields = new ArrayList<>();
        Set<String> seenFields = new HashSet<>();

        List<String> outputFields = new ArrayList<>();
        outputFields.addAll(DataFlowUtils.getFieldNames(lhsFields));
        Map<String, FieldMetadata> nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(lhsFields);
        for (String fieldName : outputFields) {
            seenFields.add(fieldName);
            declaredFields.add(nameToFieldMetadataMap.get(fieldName));
        }

        outputFields = new ArrayList<>();
        outputFields.addAll(DataFlowUtils.getFieldNames(rhsFields));
        nameToFieldMetadataMap = DataFlowUtils.getFieldMetadataMap(rhsFields);
        for (String fieldName : outputFields) {
            String originalFieldName = fieldName;

            if (seenFields.contains(fieldName)) {
                fieldName = joinFieldName(pipeName, fieldName);
                if (seenFields.contains(fieldName)) {
                    throw new RuntimeException(String.format(
                            "Cannot create joinFieldName %s from field name %s because a field with that name already exists.  Discard the field to avoid this error",
                            fieldName, originalFieldName));
                }
            }
            seenFields.add(fieldName);
            FieldMetadata origfm = nameToFieldMetadataMap.get(originalFieldName);
            FieldMetadata fm = new FieldMetadata(origfm.getAvroType(), origfm.getJavaType(), fieldName,
                    origfm.getField(), origfm.getProperties(), null);
            declaredFields.add(fm);
        }
        return declaredFields;
    }

    protected static String joinFieldName(String pipeName, String fieldName) {
        return pipeName.replaceAll("\\*|-", "__") + "__" + fieldName;
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

    public static List<FieldMetadata> retainOnlyTheseFields(FieldList outputFields, List<FieldMetadata> fm) {
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
        System.out.println("Getting Path for " + path + " " + FileSystem.FS_DEFAULT_NAME_KEY);

        if (path.startsWith(FileSystem.FS_DEFAULT_NAME_KEY)) {
            path = path.substring(7);
        } else if (path.startsWith(HdfsConstants.HDFS_URI_SCHEME + "://")) {
            String[] parts = path.split("/");
            // skip over hdfs://hostname:port/
            List<String> partsSkipped = new ArrayList<>();
            for (int i = 3; i < parts.length; ++i) {
                partsSkipped.add(parts[i]);
            }
            path = "/" + Joiner.on("/").join(partsSkipped);
        }

        return configuration.get(FileSystem.FS_DEFAULT_NAME_KEY) + path;
    }
}
