package com.latticeengines.propdata.dataflow.merge;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import cascading.operation.Buffer;

import com.latticeengines.dataflow.runtime.cascading.propdata.AlexaFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.AlexaIndustryBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.DateTimeCleanupFunction;
import com.latticeengines.domain.exposed.propdata.dataflow.MostRecentDataFlowParameters;

@Component("alexaRefreshFlow")
public class AlexaRefreshFlow extends MostRecentFlow {

    private static final String DOMAIN = "URL";
    public static final String[] COUNTRY_FIELDS = AlexaFunction.OUTPUT_FIELDS;
    private List<String> sourceFields;

    @Override
    public Node construct(MostRecentDataFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        source = source.filter("Rank == null || Rank > 0", new FieldList("Rank"));
        source = findMostRecent(source, parameters);

        DateTimeCleanupFunction function = new DateTimeCleanupFunction("OnlineSince");
        source = source.apply(function, new FieldList("OnlineSince"), new FieldMetadata("OnlineSince", Long.class));

        sourceFields = source.getFieldNames();
        Node industry = standardIndustry(source);
        Node country = getCountryRanks(source);
        FieldList joinField = new FieldList(DOMAIN);
        Node join = country.join(joinField, industry, joinField, JoinType.OUTER);
        return retainFields(join);
    }

    private Node retainFields(Node node) {
        List<String> retainedFields = new ArrayList<>(sourceFields);
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

        Collections.addAll(retainedFields, COUNTRY_FIELDS);
        retainedFields.add(AlexaIndustryBuffer.BUSINESS_INDUSTRY);
        retainedFields.add(AlexaIndustryBuffer.BUSINESS_INDUSTRY_2);

        return node.retain(new FieldList(retainedFields.toArray(new String[retainedFields.size()])));
    }

    private Node getCountryRanks(Node node) {
        List<String> argFields = new ArrayList<>();
        List<String> retFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> targetMetadata = new ArrayList<>();

        for (String countryField : COUNTRY_FIELDS) {
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
        Collections.addAll(retFields, COUNTRY_FIELDS);

        FieldList applyToFieldList = new FieldList(argFields.toArray(new String[argFields.size()]));
        FieldList outputFieldList = new FieldList(retFields.toArray(new String[retFields.size()]));
        node = node.apply(new AlexaFunction(), applyToFieldList, targetMetadata, outputFieldList);

        return node.renamePipe("country-code");
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
        outputMetadata.add(new FieldMetadata(DOMAIN, String.class));
        outputMetadata.add(new FieldMetadata(AlexaIndustryBuffer.BUSINESS_INDUSTRY, String.class));
        outputMetadata.add(new FieldMetadata(AlexaIndustryBuffer.BUSINESS_INDUSTRY_2, String.class));

        Buffer<?> industryBuffer = new AlexaIndustryBuffer(DOMAIN, industryMap, subIndustryMap);
        node = node.groupByAndBuffer(new FieldList(DOMAIN), industryBuffer, outputMetadata);

        return node.renamePipe("industries");
    }

}
