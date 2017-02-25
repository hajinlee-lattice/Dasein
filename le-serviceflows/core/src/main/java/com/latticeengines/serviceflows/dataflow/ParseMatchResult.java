package com.latticeengines.serviceflows.dataflow;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_DEDUPE_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_LOC_CHECKSUM;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_POPULATED_ATTRS;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.INT_LDC_PREMATCH_DOMAIN;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.SOURCE_PREFIX;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.TMP_BEST_DEDUPE_ID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.MatchDedupeIdAggregator;
import com.latticeengines.domain.exposed.datacloud.match.ParseMatchResultParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.expression.ExpressionFunction;
import cascading.tuple.Fields;

@Component("parseMatchResult")
public class ParseMatchResult extends TypesafeDataFlowBuilder<ParseMatchResultParameters> {

    private static final Log log = LogFactory.getLog(ParseMatchResult.class);
    private List<String> sourceCols;

    @Override
    public Node construct(ParseMatchResultParameters parameters) {
        sourceCols = parameters.sourceColumns;
        Node source = addSource(parameters.sourceTableName);

        source = resolveConflictingFields(source);

        if (needToDedupe(source)) {
            source = generateDedupeId(source);
        }

        if (parameters.excludeDataCloudAttrs) {
            List<String> fieldsToRetain = new ArrayList<>(sourceCols);
            List<String> allFields = source.getFieldNames();
            if (allFields.contains(INT_LDC_DEDUPE_ID)) {
                fieldsToRetain.add(INT_LDC_DEDUPE_ID);
            }
            source = source.retain(new FieldList(fieldsToRetain));
            // There are other possible internal attributes, like DnB Match grade
            // I belive those are only used in scoring, and this flag is only for modeling
            // so it is safe assume, we never need to retain those debug internal attrs here.
        }

        return removeInternalAttrs(source);
    }

    private Node resolveConflictingFields(Node node) {
        List<String> conflictingFields = findConflictingFields(node);
        log.warn("Found conflicting fields: " + StringUtils.join(conflictingFields, ", "));
        if (!conflictingFields.isEmpty()) {
            FieldList retainFields = retainFields(node, conflictingFields);
            node = node.retain(retainFields);
        }
        FieldList[] renameFieldLists = renameFields(conflictingFields);
        if (renameFieldLists[0].getFields().length > 0) {
            node = node.rename(renameFieldLists[0], renameFieldLists[1]);
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
        FieldList fieldsWithOutPrefix = new FieldList(conflictingFields);
        String[] namesWithPrefix = new String[conflictingFields.size()];
        for (int i = 0; i < conflictingFields.size(); i++) {
            namesWithPrefix[i] = SOURCE_PREFIX + conflictingFields.get(i);
        }
        FieldList fieldsWithPrefix = new FieldList(namesWithPrefix);
        return new FieldList[] { fieldsWithPrefix, fieldsWithOutPrefix };
    }

    private boolean needToDedupe(Node source) {
        return source.getFieldNames().contains(INT_LDC_PREMATCH_DOMAIN)
                && source.getFieldNames().contains(INT_LDC_LOC_CHECKSUM)
                && source.getFieldNames().contains(INT_LDC_POPULATED_ATTRS);
    }

    private Node generateDedupeId(Node node) {
        node = initializeDedupeId(node);
        Node matched = node.filter(INT_LDC_LID + " != null", new FieldList(INT_LDC_LID)).renamePipe("matched");

        // find best dedupe id for each matched domain
        Node unmatchedDomain = node.filter(INT_LDC_LID + " == null && " + INT_LDC_PREMATCH_DOMAIN + " != null",
                new FieldList(INT_LDC_LID, INT_LDC_PREMATCH_DOMAIN));
        unmatchedDomain = updateByBestIdInGroup(unmatchedDomain, matched, INT_LDC_PREMATCH_DOMAIN);

        // find best dedupe id for each match location, without domain
        Node unmatchedLocOnly = node.filter(INT_LDC_LID + " == null && " + INT_LDC_PREMATCH_DOMAIN + " == null",
                new FieldList(INT_LDC_LID, INT_LDC_PREMATCH_DOMAIN));
        unmatchedLocOnly = updateByBestIdInGroup(unmatchedLocOnly, matched, INT_LDC_LOC_CHECKSUM);

        unmatchedLocOnly.orderFields(new FieldList(unmatchedDomain.getFieldNames()));
        return matched.merge(Arrays.asList(unmatchedDomain, unmatchedLocOnly));
    }

    private Node initializeDedupeId(Node node) {
        String expression = String.format(
                "%s == null ? " + //
                        "(%s == null ? \"\" : %s) + \"\" + (%s == null ? \"\" : %s) : " + //
                        "String.valueOf(%s)",
                INT_LDC_LID, //
                INT_LDC_PREMATCH_DOMAIN, INT_LDC_PREMATCH_DOMAIN, //
                INT_LDC_LOC_CHECKSUM, INT_LDC_LOC_CHECKSUM, //
                INT_LDC_LID);
        Function function = new ExpressionFunction(new Fields(INT_LDC_DEDUPE_ID), //
                expression, //
                new String[] { INT_LDC_LID, INT_LDC_PREMATCH_DOMAIN, INT_LDC_LOC_CHECKSUM }, //
                new Class<?>[] { Long.class, String.class, String.class });
        return node.apply(function, new FieldList(INT_LDC_LID, INT_LDC_PREMATCH_DOMAIN, INT_LDC_LOC_CHECKSUM),
                new FieldMetadata(INT_LDC_DEDUPE_ID, String.class));
    }

    private Node updateByBestIdInGroup(Node unmatched, Node matched, String grpField) {
        Aggregator aggregator = new MatchDedupeIdAggregator(new Fields(grpField, TMP_BEST_DEDUPE_ID));
        List<FieldMetadata> bufferFms = new ArrayList<>();
        bufferFms.add(new FieldMetadata(INT_LDC_PREMATCH_DOMAIN, String.class));
        bufferFms.add(new FieldMetadata(TMP_BEST_DEDUPE_ID, String.class));
        Node bestIdInGroup = matched.groupByAndAggregate(new FieldList(grpField), aggregator, bufferFms)
                .renamePipe("bestidfor" + grpField);
        unmatched = unmatched.leftOuterJoin(new FieldList(grpField), bestIdInGroup, new FieldList(grpField));

        String tmpField = UUID.randomUUID().toString().replace("-", "");
        Function updateIdByDomain = new ExpressionFunction(new Fields(tmpField), //
                String.format("%s == null ? %s : %s", TMP_BEST_DEDUPE_ID, INT_LDC_DEDUPE_ID, TMP_BEST_DEDUPE_ID), //
                new String[] { TMP_BEST_DEDUPE_ID, INT_LDC_DEDUPE_ID }, //
                new Class<?>[] { String.class, String.class });
        unmatched = unmatched.apply(updateIdByDomain, new FieldList(TMP_BEST_DEDUPE_ID, INT_LDC_DEDUPE_ID),
                new FieldMetadata(tmpField, String.class));
        unmatched = unmatched.discard(new FieldList(INT_LDC_DEDUPE_ID)) //
                .rename(new FieldList(tmpField), new FieldList(INT_LDC_DEDUPE_ID)) //
                .retain(new FieldList(matched.getFieldNamesArray())).renamePipe("unmatched" + grpField);
        return unmatched;
    }

    private Node removeInternalAttrs(Node node) {
        List<String> internalAttrs = new ArrayList<>();
        if (node.getFieldNames().contains(INT_LDC_LID)) {
            internalAttrs.add(INT_LDC_LID);
        }
        if (node.getFieldNames().contains(INT_LDC_POPULATED_ATTRS)) {
            internalAttrs.add(INT_LDC_POPULATED_ATTRS);
        }
        if (node.getFieldNames().contains(INT_LDC_LOC_CHECKSUM)) {
            internalAttrs.add(INT_LDC_LOC_CHECKSUM);
        }
        if (node.getFieldNames().contains(INT_LDC_PREMATCH_DOMAIN)) {
            internalAttrs.add(INT_LDC_PREMATCH_DOMAIN);
        }
        if (!internalAttrs.isEmpty()) {
            node = node.discard(new FieldList(internalAttrs));
        }
        return node;
    }

}
