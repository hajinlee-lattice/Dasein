package com.latticeengines.datacloud.dataflow.transformation.source;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.MappingFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.ConsolidateCollectionParameters;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(ConsolidateCollectionOrbFlow.BEAN_NAME)
public class ConsolidateCollectionOrbFlow extends ConsolidateCollectionFlow {
    public static final String BEAN_NAME = "consolidateCollectionOrbFlow";

    private static final String FIELD_YEAR_FOUNDED = "Year_Founded";
    private static final String FIELD_REVENUE_RANGE = "BusinessRevenueRange";
    private static final String FIELD_INDUSTRY = "Industry";
    private static final String FIELD_CONSOLIDATED_INDUSTRY = "ConsolidatedIndustry";
    private static final String FIELD_BUSINESS_INDUSTRY = "BusinessIndustry";
    private static final String FIELD_BUSINESS_INDUSTRY2 = "BusinessIndustry2";

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

    @Override
    protected Node preRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        return renameFields(src);
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

        return node.apply(new MappingFunction(FIELD_INDUSTRY, FIELD_CONSOLIDATED_INDUSTRY, industryMap), new FieldList(FIELD_INDUSTRY),
                new FieldMetadata(FIELD_CONSOLIDATED_INDUSTRY, String.class));
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

        node = node.apply(new MappingFunction(FIELD_INDUSTRY, FIELD_BUSINESS_INDUSTRY, industryMap), new FieldList(FIELD_INDUSTRY),
                new FieldMetadata(FIELD_BUSINESS_INDUSTRY, String.class));

        node = node.apply(new MappingFunction(FIELD_INDUSTRY, FIELD_BUSINESS_INDUSTRY2, subIndustryMap), new FieldList(FIELD_INDUSTRY),
                new FieldMetadata(FIELD_BUSINESS_INDUSTRY2, String.class));

        return node;
    }

    private Node addUrlExistsField(Node node, String urlField) {
        String booleanField = urlField + "_Exists";
        node = node.apply(urlField + " != null ? true : false", new FieldList(urlField),
                new FieldMetadata(booleanField, Boolean.class));
        return node.renameBooleanField(booleanField, BooleanType.Y_N);
    }

    private Node removeUnusedFields(Node src) {

        List<String> fields = src.getFieldNames();
        fields.removeAll(Arrays.asList("LEInternalID", "LEParentInternalID"));

        return src.retain(new FieldList(fields));

    }

    @Override
    protected Node postRecentTransform(Node src, ConsolidateCollectionParameters parameters) {
        src = src.apply(
                String.format("%s == null ? null : String.valueOf(%s)", FIELD_YEAR_FOUNDED, FIELD_YEAR_FOUNDED), new FieldList(
                        FIELD_YEAR_FOUNDED), new FieldMetadata(FIELD_YEAR_FOUNDED, String.class));

        src = src.apply(
                String.format(
                        "(\"0 - 1M\".equals(%s) || \"1M - 10M\".equals(%s)) ? \"0 - 10M\" : %s", FIELD_REVENUE_RANGE, FIELD_REVENUE_RANGE,
                        FIELD_REVENUE_RANGE), new FieldList(FIELD_REVENUE_RANGE), new FieldMetadata(FIELD_REVENUE_RANGE, String.class));

        src = consolidateIndustry(src);
        src = standardIndustry(src);

        src = addUrlExistsField(src, "Facebook_Url");
        src = addUrlExistsField(src, "GooglePlus_Url");
        src = addUrlExistsField(src, "LinkedIn_Url");
        src = addUrlExistsField(src, "Twitter_Url");

        src = removeUnusedFields(src);

        if (parameters.getBaseTables().size() == 2) {

            Node legacy = addSource(parameters.getBaseTables().get(1)).retain(src.getFieldNamesArray());

            return findMostRecent(src.merge(legacy), parameters);

        } else {

            return src;

        }
    }
}
