package com.latticeengines.serviceflows.dataflow.match;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainMergeAndCleanFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.DunsMergeFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.MatchIDGenerationFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.CascadingBulkMatchDataflowParameters;
import com.latticeengines.domain.exposed.datacloud.dataflow.DecodedPair;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("cascadingBulkMatchDataflow")
public class CascadingBulkMatchDataflow extends TypesafeDataFlowBuilder<CascadingBulkMatchDataflowParameters> {
    private static final Log log = LogFactory.getLog(CascadingBulkMatchDataflow.class);

    public static final String MATCH_ID_KEY = "__MATCH_ID__";
    public static final String PARSED_DOMAIN = "__PARSED_DOMAIN__";
    public static final String PARSED_DUNS = "__PARSED_DUNS__";

    private String LOOKUP_KEY_FIELDNAME = "Key";
    private String LATTICE_ID_FIELDNAME = "LatticeID";
    private String PUBLIC_DOMAIN_FIELDNAME = "Domain";
    private String PUBLIC_DOMAIN_NEW_FIELDNAME = "__Public_Domain__";

    @Override
    public Node construct(CascadingBulkMatchDataflowParameters parameters) {
        FieldList latticeIdField = new FieldList(LATTICE_ID_FIELDNAME);

        Node inputSource = addSource(parameters.getInputAvro());
        List<FieldMetadata> inputMetadata = inputSource.getSchema();

        Node accountMasterLookupSource = addSource(parameters.getAccountMasterLookup());
        Node matchedLookupNode = matchLookup(parameters, inputSource, accountMasterLookupSource);
        Node matchedNode = matchAccountMaster(parameters, latticeIdField, inputMetadata, matchedLookupNode);
        return matchedNode;

    }

    private Node matchAccountMaster(CascadingBulkMatchDataflowParameters parameters, FieldList latticeIdField,
            List<FieldMetadata> inputMetadata, Node matchedLookupNode) {

        Node accountMasterSource = addSource(parameters.getAccountMaster());
        List<List<String>> outputList = buildOutputFieldList(inputMetadata, parameters, accountMasterSource);
        List<String> predefinedFields = outputList.get(1);
        predefinedFields.addAll(outputList.get(2));
        System.out.println("Predefined columns=" + predefinedFields);
        accountMasterSource = accountMasterSource.retain(new FieldList(predefinedFields));

        Node matchedLookupIdNode = matchedLookupNode.retain(latticeIdField);
        matchedLookupIdNode = matchedLookupIdNode.groupByAndLimit(latticeIdField, 1);
        Node hashMatchedNode = accountMasterSource.hashJoin(latticeIdField, matchedLookupIdNode, latticeIdField,
                JoinType.INNER);

        JoinType joinType = parameters.getReturnUnmatched() ? JoinType.LEFT : JoinType.INNER;
        matchedLookupNode = matchedLookupNode.join(latticeIdField, hashMatchedNode, latticeIdField, joinType);

        matchedLookupNode = decodeColumns(matchedLookupNode, parameters, outputList.get(2));

        List<String> resultFields = outputList.get(0);
        log.info("output fields=" + resultFields);
        List<FieldMetadata> fieldMetadata = getOutputMetadata(inputMetadata, parameters, resultFields);
        matchedLookupNode.setSchema(fieldMetadata);
        matchedLookupNode = matchedLookupNode.retain(new FieldList(resultFields.toArray(new String[0])));

        return matchedLookupNode;
    }

    private List<FieldMetadata> getOutputMetadata(List<FieldMetadata> inputMetadatas,
            CascadingBulkMatchDataflowParameters parameters, List<String> resultFields) {
        List<FieldMetadata> metadatas = getMetadataFromSchemaPath(parameters.getOutputSchemaPath());
        List<FieldMetadata> newMetadatas = new ArrayList<>();
        newMetadatas.addAll(inputMetadatas);
        for (FieldMetadata metadata : metadatas) {
            if (resultFields.contains(metadata.getFieldName())) {
                newMetadatas.add(metadata);
            }
        }
        return newMetadatas;
    }

    private Node decodeColumns(Node matchedLookupNode, CascadingBulkMatchDataflowParameters parameters,
            List<String> encodedColumns) {
        Map<String, DecodedPair> decodedParameters = parameters.getDecodedParameters();
        if (decodedParameters != null && decodedParameters.size() > 0) {
            for (Map.Entry<String, DecodedPair> entry : decodedParameters.entrySet()) {
                if (encodedColumns.contains(entry.getKey()) && entry.getValue() != null
                        && entry.getValue().getBitCodeBook() != null && entry.getValue().getDecodedColumns() != null) {
                    log.info("Eecode column=" + entry.getKey() + ", Decoded columns' count="
                            + entry.getValue().getDecodedColumns().size());
                    matchedLookupNode = matchedLookupNode.bitDecode(entry.getKey(), entry.getValue()
                            .getDecodedColumns().toArray(new String[0]), entry.getValue().getBitCodeBook());
                }
            }
        }
        return matchedLookupNode;
    }

    private List<List<String>> buildOutputFieldList(List<FieldMetadata> inputMetadata,
            CascadingBulkMatchDataflowParameters parameters, Node accountMasterSource) {
        List<String> outputFields = new ArrayList<>();
        Set<String> inputputFieldSet = new HashSet<>();
        for (FieldMetadata fieldMetadata : inputMetadata) {
            outputFields.add(fieldMetadata.getFieldName());
            inputputFieldSet.add(fieldMetadata.getFieldName().toLowerCase());
        }

        List<String> accountMasterFieldNames = accountMasterSource.getFieldNames();
        Set<String> accountMasterFieldSet = new HashSet<>(accountMasterFieldNames);
        FieldList predefinedFieldList = buildFieldListFromSchema(parameters.getOutputSchemaPath());
        List<String> predefinedFields = new ArrayList<>();
        predefinedFields.add(LATTICE_ID_FIELDNAME);
        List<String> encodedColumns = new ArrayList<>();
        Set<String> decodedColumnSet = getDecodedColumns(parameters, accountMasterFieldSet, encodedColumns);
        for (String predefinedField : predefinedFieldList.getFields()) {
            if (!inputputFieldSet.contains(predefinedField.toLowerCase())
                    && (accountMasterFieldSet.contains(predefinedField) || decodedColumnSet.contains(predefinedField))) {
                if (accountMasterFieldSet.contains(predefinedField)) {
                    predefinedFields.add(predefinedField);
                }
                outputFields.add(predefinedField);
            }
            if (!accountMasterFieldSet.contains(predefinedField) && !decodedColumnSet.contains(predefinedField)) {
                log.warn("Missing predefined field in Account Master file or Encoded columns:" + predefinedField);
            }
        }
        List<List<String>> outputList = new ArrayList<List<String>>();
        outputList.add(outputFields);
        outputList.add(predefinedFields);
        outputList.add(encodedColumns);
        return outputList;
    }

    private Set<String> getDecodedColumns(CascadingBulkMatchDataflowParameters parameters,
            Set<String> accountMasterFieldSet, List<String> encodedColumns) {
        Set<String> decodedColumns = new HashSet<>();
        if (parameters.getDecodedParameters() != null) {
            for (Map.Entry<String, DecodedPair> entry : parameters.getDecodedParameters().entrySet()) {
                if (accountMasterFieldSet.contains(entry.getKey())) {
                    decodedColumns.addAll(entry.getValue().getDecodedColumns());
                    encodedColumns.add(entry.getKey());
                } else {
                    log.warn("Missing decoded column=" + entry.getKey());
                }
            }
        }
        return decodedColumns;
    }

    private Node matchLookup(CascadingBulkMatchDataflowParameters parameters, Node inputSource,
            Node accountMasterLookupSource) {

        Map<MatchKey, List<String>> keyMap = parameters.getKeyMap();
        Node matchedLookupNode = processDomain(parameters, inputSource, keyMap);
        matchedLookupNode = generateMatchKey(keyMap, matchedLookupNode);

        FieldList matchIdField = new FieldList(MATCH_ID_KEY);
        Node matchedLookupIdNode = matchedLookupNode.retain(matchIdField);
        matchedLookupIdNode = matchedLookupIdNode.groupByAndLimit(matchIdField, 1);
        Node hashMatchedLookupNode = accountMasterLookupSource.hashJoin(new FieldList(LOOKUP_KEY_FIELDNAME),
                matchedLookupIdNode, matchIdField, JoinType.INNER);

        JoinType joinType = parameters.getReturnUnmatched() ? JoinType.LEFT : JoinType.INNER;
        matchedLookupNode = matchedLookupNode.join(matchIdField, hashMatchedLookupNode, matchIdField, joinType);
        return matchedLookupNode;
    }

    private Node generateMatchKey(Map<MatchKey, List<String>> keyMap, Node matchedLookupNode) {
        List<String> keyFields = new ArrayList<>();
        if (keyMap != null && CollectionUtils.isNotEmpty(keyMap.get(MatchKey.Domain))) {
            keyFields.add(PARSED_DOMAIN);
        }
        List<String> dunsFieldNames = keyMap.get(MatchKey.DUNS);
        if (CollectionUtils.isNotEmpty(dunsFieldNames)) {
            if (dunsFieldNames.size() > 1) {
                matchedLookupNode = matchedLookupNode.apply(new DunsMergeFunction(dunsFieldNames, PARSED_DUNS),
                        new FieldList(dunsFieldNames), new FieldMetadata(PARSED_DUNS, String.class));
                keyFields.add(PARSED_DUNS);
            } else {
                keyFields.add(dunsFieldNames.get(0));
            }
        }
        if (keyFields.size() == 0) {
            throw new RuntimeException("There's no Domain or Duns field in input file!");
        }
        matchedLookupNode = matchedLookupNode.apply(new MatchIDGenerationFunction(keyFields, MATCH_ID_KEY),
                new FieldList(keyFields), new FieldMetadata(MATCH_ID_KEY, String.class));
        return matchedLookupNode;
    }

    private Node processDomain(CascadingBulkMatchDataflowParameters parameters, Node inputSource,
            Map<MatchKey, List<String>> keyMap) {

        Node parsedDomainNode = inputSource;
        if (keyMap != null && CollectionUtils.isNotEmpty(keyMap.get(MatchKey.Domain))) {
            List<String> domainFieldNames = keyMap.get(MatchKey.Domain);
            parsedDomainNode = parsedDomainNode.apply(new DomainMergeAndCleanFunction(domainFieldNames, PARSED_DOMAIN),
                    new FieldList(domainFieldNames), new FieldMetadata(PARSED_DOMAIN, String.class));

            if (parameters.getExcludePublicDomains() && StringUtils.isNotBlank(parameters.getPublicDomainPath())) {
                log.info("Starting to exclude public domain. file =" + parameters.getPublicDomainPath());
                Node publicDomainSource = addSource(parameters.getPublicDomainPath());
                publicDomainSource = publicDomainSource.rename(new FieldList(PUBLIC_DOMAIN_FIELDNAME), new FieldList(
                        PUBLIC_DOMAIN_NEW_FIELDNAME));
                parsedDomainNode = parsedDomainNode.stopList(publicDomainSource, PARSED_DOMAIN,
                        PUBLIC_DOMAIN_NEW_FIELDNAME);
            }
        }
        return parsedDomainNode;
    }
}
