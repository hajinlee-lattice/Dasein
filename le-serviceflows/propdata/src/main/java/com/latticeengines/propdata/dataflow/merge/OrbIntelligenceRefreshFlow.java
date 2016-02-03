package com.latticeengines.propdata.dataflow.merge;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.propdata.MostRecentDataFlowParameters;

@Component("orbIntelligenceRefreshFlow")
public class OrbIntelligenceRefreshFlow extends MostRecentFlow  {

    private static final String YEAR_FOUNDED = "Year_Founded";
    private static final String REVENUE_RANGE = "BusinessRevenueRange";
    private static final String INDUSTRY = "Industry";
    private static final String CONSOLIDATED_INDUSTRY = "ConsolidatedIndustry";
    private static final String BUSINESS_INDUSTRY = "BusinessIndustry";
    private static final String BUSINESS_INDUSTRY2 = "BusinessIndustry2";

    @Override
    public Node construct(MostRecentDataFlowParameters parameters) {
        Node source = addSource(parameters.getSourceTable());
        source = renameFields(source);
        source = findMostRecent(source, parameters);

        source = source.addFunction(
                String.format("%s == null ? null : String.valueOf(%s)", YEAR_FOUNDED, YEAR_FOUNDED),
                new FieldList(YEAR_FOUNDED), new FieldMetadata(YEAR_FOUNDED, String.class));

        source = source.addFunction(
                String.format("(\"0 - 1M\".equals(%s) || \"1M - 10M\".equals(%s)) ? \"0 - 10M\" : %s",
                        REVENUE_RANGE, REVENUE_RANGE, REVENUE_RANGE),
                new FieldList(REVENUE_RANGE), new FieldMetadata(REVENUE_RANGE, String.class));

        source = consolidateIndustry(source);
        source = standardIndustry(source);

        source = addUrlExistsField(source, "Facebook_Url");
        source = addUrlExistsField(source, "GooglePlus_Url");
        source = addUrlExistsField(source, "LinkedIn_Url");
        source = addUrlExistsField(source, "Twitter_Url");

        return source;
    }

    private Node addUrlExistsField(Node node, String urlField) {
        String booleanField = urlField + "_Exists";
        node = node.addFunction(urlField + " != null ? true : false", new FieldList(urlField),
                new FieldMetadata(booleanField, Boolean.class));
        return node.renameBooleanField(booleanField, BooleanType.Y_N);
    }

    private Node renameFields(Node node) {
        List<String> originalNames = new ArrayList<>();
        List<String> targetNames = new ArrayList<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("collection/orb_column_mapping.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource orb_column_mapping.txt");
        }
        Scanner scanner = new Scanner(is);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] pair = line.split(",");
            originalNames.add(pair[0]);
            targetNames.add(pair[1]);
        }
        scanner.close();

        return node.rename(new FieldList(originalNames), new FieldList(targetNames));
    }

    private Node consolidateIndustry(Node node) {
        Map<Serializable, Serializable> industryMap = new HashMap<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("collection/orb_industry_mapping.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource orb_industry_mapping.txt");
        }
        Scanner scanner = new Scanner(is);

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] pair = line.split("\\|");
            industryMap.put(pair[0], pair[1]);
        }
        scanner.close();

        return node.apply(new MappingFunction(INDUSTRY, CONSOLIDATED_INDUSTRY, industryMap),
                new FieldList(INDUSTRY), new FieldMetadata(CONSOLIDATED_INDUSTRY, String.class));
    }

    private Node standardIndustry(Node node) {
        Map<Serializable, Serializable> industryMap = new HashMap<>();
        Map<Serializable, Serializable> subIndustryMap = new HashMap<>();

        InputStream is = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("collection/standard_industry_mapping.txt");
        if (is == null) {
            throw new RuntimeException("Cannot find resource standard_industry_mapping.txt");
        }
        Scanner scanner = new Scanner(is);

        int orbIndustryPos = 2;
        int industryPos = 0;
        int subIndustryPos = 1;

        while (scanner.hasNextLine()) {
            String line = scanner.nextLine();
            String[] tokens = line.split("\\|");
            String industry = tokens[orbIndustryPos].trim();
            if (StringUtils.isNotEmpty(industry)) {
                industryMap.put(industry, tokens[industryPos]);
                subIndustryMap.put(industry, tokens[subIndustryPos]);
            }
        }
        scanner.close();

        node = node.apply(new MappingFunction(INDUSTRY, BUSINESS_INDUSTRY, industryMap),
                new FieldList(INDUSTRY), new FieldMetadata(BUSINESS_INDUSTRY, String.class));

        node = node.apply(new MappingFunction(INDUSTRY, BUSINESS_INDUSTRY2, subIndustryMap),
                new FieldList(INDUSTRY), new FieldMetadata(BUSINESS_INDUSTRY2, String.class));

        return node;
    }

}
