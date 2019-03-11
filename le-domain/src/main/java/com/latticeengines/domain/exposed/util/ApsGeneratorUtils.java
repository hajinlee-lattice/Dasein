package com.latticeengines.domain.exposed.util;

import java.util.List;
import java.util.Map;
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

    public static void setupMetaData(Table apsTable, Map<String, List<Product>> productMap) {
        List<Attribute> attributes = apsTable.getAttributes();
        for (Attribute attribute : attributes) {
            String name = attribute.getName();
            if ("LEAccount_ID".equalsIgnoreCase(name) || "Period_ID".equalsIgnoreCase(name)
                    || InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                attribute.setApprovedUsage(ApprovedUsage.NONE);
                attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
                attribute.setCategory(Category.DEFAULT);
                if (InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                    attribute.setLogicalDataType(LogicalDataType.InternalId);
                }
                log.info(String.format("Setting category for %s attribute as %s", name,
                        Category.DEFAULT.getName()));
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
        if (name.matches("Product_.*_Revenue")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Revenue");
            setDisplayName(attribute, name, productMap, pattern, "Last Period Spend for ");
            attribute.setDescription("Product spend in last period");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches("Product_.*_RevenueRollingSum6")) {
            Pattern pattern = Pattern.compile("Product_(.*)_RevenueRollingSum6");
            setDisplayName(attribute, name, productMap, pattern, "6-Period Spend for ");
            attribute.setDescription("Product spend for last 6 periods");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches("Product_.*_RevenueMomentum3")) {
            Pattern pattern = Pattern.compile("Product_(.*)_RevenueMomentum3");
            setDisplayName(attribute, name, productMap, pattern,
                    "Rate of Change of 3-Period Spend for ");
            attribute.setDescription(
                    "Percent change in the 3 period spend, where values > 0 show increasing spend and < 0 indicate decreasing spend");
            return;
        }
        if (name.matches("Product_.*_Units")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Units");
            setDisplayName(attribute, name, productMap, pattern, "Last Period Units for ");
            attribute.setDescription("Units purchased in last period");
            return;
        }
        if (name.matches("Product_.*_Span")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Span");
            setDisplayName(attribute, name, productMap, pattern, "Purchase Recency for ");
            attribute.setDescription(
                    "Indicator of how recently a customer purchased, where higher values indicate more recent purchase (e.g. 1 = Last Period  and 0 = Never)");
            return;
        }
    }

    private static void setDisplayName(Attribute attribute, String name,
            Map<String, List<Product>> productMap, Pattern pattern, String descPrefix) {
        Matcher matcher = pattern.matcher(name);
        matcher.matches();
        String productId = matcher.group(1);
        if (productMap != null && CollectionUtils.isNotEmpty(productMap.get(productId))) {
            attribute
                    .setDisplayName(descPrefix + productMap.get(productId).get(0).getProductName());
        } else {
            attribute.setDisplayName(descPrefix + productId);
        }
    }
}
