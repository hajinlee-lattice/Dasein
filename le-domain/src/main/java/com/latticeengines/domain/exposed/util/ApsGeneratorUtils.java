package com.latticeengines.domain.exposed.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class ApsGeneratorUtils {

    private static final Logger log = LoggerFactory.getLogger(ApsGeneratorUtils.class);

    private static final String PRODUCT_SPEND_IN_LAST_PERIOD_PATTERN = "Product_(.*)_Revenue";
    private static final String PRODUCT_SPEND_IN_6_PRERIOD_PATTERN = "Product_(.*)_RevenueRollingSum6";
    private static final String PRODUCT_SPEND_IN_3_PRERIOD_PATTERN = "Product_(.*)_RevenueMomentum3";
    private static final String UNITS_PURCHASED_PATTERN = "Product_(.*)_Units";
    private static final String PRODUCT_SPAN_PATTERN = "Product_(.*)_Span";

    public static final Set<String> ApsNamePatternSet = new HashSet<>(
            Arrays.asList(PRODUCT_SPEND_IN_LAST_PERIOD_PATTERN, PRODUCT_SPEND_IN_6_PRERIOD_PATTERN,
                    PRODUCT_SPEND_IN_3_PRERIOD_PATTERN, UNITS_PURCHASED_PATTERN, PRODUCT_SPAN_PATTERN));

    public static void setupMetaData(Table apsTable, Map<String, List<Product>> productMap) {
        List<Attribute> attributes = apsTable.getAttributes();
        for (Attribute attribute : attributes) {
            String name = attribute.getName();
            if (InterfaceName.LEAccount_ID.name().equalsIgnoreCase(name)
                    || InterfaceName.Period_ID.name().equalsIgnoreCase(name)
                    || InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                attribute.setApprovedUsage(ApprovedUsage.NONE);
                attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
                attribute.setCategory(Category.DEFAULT);
                if (InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                    attribute.setLogicalDataType(LogicalDataType.InternalId);
                }
                log.info(String.format("Setting category for %s attribute as %s", name, Category.DEFAULT.getName()));
            } else {
                attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
                attribute.setCategory(Category.PRODUCT_SPEND);
                attribute.setDisplayDiscretizationStrategy("{\"unified\": {}}");
                attribute.setTags(ModelingMetadata.INTERNAL_TAG);
                setDisplayNameAndOthers(attribute, name, productMap);
            }
        }
    }

    private static void setDisplayNameAndOthers(Attribute attribute, String name,
            Map<String, List<Product>> productMap) {
        if (name.matches(PRODUCT_SPEND_IN_LAST_PERIOD_PATTERN)) {
            Pattern pattern = Pattern.compile(PRODUCT_SPEND_IN_LAST_PERIOD_PATTERN);
            setDisplayNameAndSubcategory(attribute, name, productMap, pattern, "Last Period Spend for ");
            attribute.setDescription("Product spend in last period");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches(PRODUCT_SPEND_IN_6_PRERIOD_PATTERN)) {
            Pattern pattern = Pattern.compile(PRODUCT_SPEND_IN_6_PRERIOD_PATTERN);
            setDisplayNameAndSubcategory(attribute, name, productMap, pattern, "6-Period Spend for ");
            attribute.setDescription("Product spend for last 6 periods");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches(PRODUCT_SPEND_IN_3_PRERIOD_PATTERN)) {
            Pattern pattern = Pattern.compile(PRODUCT_SPEND_IN_3_PRERIOD_PATTERN);
            setDisplayNameAndSubcategory(attribute, name, productMap, pattern, "Rate of Change of 3-Period Spend for ");
            attribute.setDescription(
                    "Percent change in the 3 period spend, where values > 0 show increasing spend and < 0 indicate decreasing spend");
            return;
        }
        if (name.matches(UNITS_PURCHASED_PATTERN)) {
            Pattern pattern = Pattern.compile(UNITS_PURCHASED_PATTERN);
            setDisplayNameAndSubcategory(attribute, name, productMap, pattern, "Last Period Units for ");
            attribute.setDescription("Units purchased in last period");
            return;
        }
        if (name.matches(PRODUCT_SPAN_PATTERN)) {
            Pattern pattern = Pattern.compile(PRODUCT_SPAN_PATTERN);
            setDisplayNameAndSubcategory(attribute, name, productMap, pattern, "Purchase Recency for ");
            attribute.setDescription(
                    "Indicator of how recently a customer purchased, where higher values indicate more recent purchase (e.g. 1 = Last Period  and 0 = Never)");
            return;
        }
    }

    public static boolean isApsAttr(String name) {
        return ApsNamePatternSet.stream().anyMatch(p -> name.matches(p));
    }

    private static void setDisplayNameAndSubcategory(Attribute attribute, String name,
            Map<String, List<Product>> productMap, Pattern pattern, String descPrefix) {
        Matcher matcher = pattern.matcher(name);
        matcher.matches();
        String productId = matcher.group(1);
        if (productMap != null && CollectionUtils.isNotEmpty(productMap.get(productId))) {
            attribute.setDisplayName(descPrefix + productMap.get(productId).get(0).getProductName());
            attribute.setSubcategory(productMap.get(productId).get(0).getProductBundle());
        } else {
            attribute.setDisplayName(descPrefix + productId);
        }
    }
}
