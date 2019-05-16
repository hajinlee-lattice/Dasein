package com.latticeengines.datacloud.dataflow.transformation.source;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AlexaFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AlexaIndustryBuffer;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Buffer;

@Component(ConsolidateCollectionAlexaFlow.BEAN_NAME)
public class ConsolidateCollectionAlexaFlow extends ConsolidateCollectionFlow {
    public static final String BEAN_NAME = "consolidateCollectionAlexaFlow";
    private static final String FIELD_DOMAIN = DataCloudConstants.ATTR_ALEXA_DOMAIN;
    private static final String[] FIELDS_COUNTRY = AlexaFunction.OUTPUT_FIELDS;
    private static final String[] FIELDS_RANK = new String[] { "Rank", "ReachRank", "ViewsRank", "US_Rank", "AU_Rank",
            "GB_Rank", "CA_Rank" };

    @Override
    protected Node preRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        return src.filter("Rank == null || Rank > 0", new FieldList("Rank"));
    }

    private Node standardIndustry(Node node) {
        Map<String, String> industryMap = new HashMap<>();
        Map<String, String> subIndustryMap = new HashMap<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("collection/alexa_industry_mapping.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource alexa_industry_mapping.txt");
        }
        Scanner scanner = new Scanner(is);

        int indPos = 0;
        int ind2Pos = 1;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] tokens = line.split("\\|");
            String industry = tokens[indPos].trim();
            String industry2 = tokens[ind2Pos].trim();
            for (int i = 2; i < tokens.length; i++) {
                String category = StringUtils.trim(tokens[i]);
                if (StringUtils.isNotEmpty(category)) {
                    industryMap.put(category, industry);
                    subIndustryMap.put(category, industry2);
                }
            }
        }
        scanner.close();

        List<FieldMetadata> outputMetadata = new ArrayList<>();
        outputMetadata.add(new FieldMetadata(FIELD_DOMAIN, String.class));
        outputMetadata.add(new FieldMetadata(AlexaIndustryBuffer.BUSINESS_INDUSTRY, String.class));
        outputMetadata.add(new FieldMetadata(AlexaIndustryBuffer.BUSINESS_INDUSTRY_2, String.class));

        Buffer<?> industryBuffer = new AlexaIndustryBuffer(FIELD_DOMAIN, industryMap, subIndustryMap);
        node = node.groupByAndBuffer(new FieldList(FIELD_DOMAIN), industryBuffer, outputMetadata);

        return node.renamePipe("industries");
    }

    private Node getCountryRanks(Node node) {
        List<String> argFields = new ArrayList<>();
        List<String> retFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> targetMetadata = new ArrayList<>();

        for (String countryField : FIELDS_COUNTRY) {
            if (countryField.contains("PageViews")) {
                targetMetadata.add(new FieldMetadata(countryField, Double.class));
            } else if (countryField.contains("Rank")) {
                targetMetadata.add(new FieldMetadata(countryField, Integer.class));
            } else if (countryField.contains("Users")) {
                targetMetadata.add(new FieldMetadata(countryField, Double.class));
            }
        }

        for (int i = 1; i <= 10; i++) {
            String countryCode = String.format("Country%d_Code", i);
            String pageViews = String.format("PageViews%d", i);
            String rank = String.format("Rank%d", i);
            String users = String.format("Users%d", i);

            argFields.add(countryCode);
            argFields.add(pageViews);
            argFields.add(rank);
            argFields.add(users);
        }

        retFields.removeAll(argFields);
        Collections.addAll(retFields, FIELDS_COUNTRY);

        FieldList applyToFieldList = new FieldList(argFields.toArray(new String[argFields.size()]));
        FieldList outputFieldList = new FieldList(retFields.toArray(new String[retFields.size()]));
        node = node.apply(new AlexaFunction(), applyToFieldList, targetMetadata, outputFieldList);

        return node.renamePipe("country-code");
    }

    private Node cleanInvalidRank(Node node) {
        for (String rankField : FIELDS_RANK) {
            node = node.apply(String.format("%s == null || %s <= 0 ? null : %s", rankField, rankField, rankField),
                    new FieldList(rankField), new FieldMetadata(rankField, Integer.class));
        }
        return node;
    }

    private Node retainFields(Node node, List<String> srcFields) {
        List<String> retainedFields = new ArrayList<>(srcFields);
        retainedFields.remove("LEInternalID");
        retainedFields.remove("LEParentInternalID");
        retainedFields.remove("Creation_Date");
        retainedFields.remove("Last_Modification_Date");

        for (int i = 1; i <= 10; i++) {
            retainedFields.remove(String.format("Country%d_Code", i));
            retainedFields.remove(String.format("PageViews%d", i));
            retainedFields.remove(String.format("Rank%d", i));
            retainedFields.remove(String.format("Users%d", i));
        }

        Collections.addAll(retainedFields, FIELDS_COUNTRY);
        retainedFields.add(AlexaIndustryBuffer.BUSINESS_INDUSTRY);
        retainedFields.add(AlexaIndustryBuffer.BUSINESS_INDUSTRY_2);

        return node.retain(new FieldList(retainedFields.toArray(new String[retainedFields.size()])));
    }

    @Override
    protected Node postRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        List<String> srcFields = src.getFieldNames();

        Node industries = standardIndustry(src);
        Node countryRanks = getCountryRanks(src);

        FieldList joinField = new FieldList(FIELD_DOMAIN);
        Node joinRet = countryRanks.join(joinField, industries, joinField, JoinType.OUTER);
        joinRet = cleanInvalidRank(joinRet);

        Node retained = retainFields(joinRet, srcFields);

        //combine legacy consolidation data
        if (parameters.getBaseTables().size() == 2) {

            Node legacy = addSource(parameters.getBaseTables().get(1)).retain(retained.getFieldNamesArray());

            return findMostRecent(retained.merge(legacy), parameters);

        } else {

            return retained;

        }

    }
}
