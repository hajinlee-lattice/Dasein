package com.latticeengines.domain.exposed.util;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;

public class AwsApsGeneratorUtils {

    public static void setupMetaData(Table apsTable) {
        List<Attribute> attributes = apsTable.getAttributes();
        for (Attribute attribute : attributes) {
            String name = attribute.getName();
            if ("LEAccount_ID".equalsIgnoreCase(name) || "Period_ID".equalsIgnoreCase(name)
                    || InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                attribute.setApprovedUsage(ApprovedUsage.NONE);
                attribute.setTags(ModelingMetadata.EXTERNAL_TAG);
                attribute.setCategory(Category.ACCOUNT_INFORMATION);
                if (InterfaceName.AnalyticPurchaseState_ID.name().equalsIgnoreCase(name)) {
                    attribute.setLogicalDataType(LogicalDataType.InternalId);
                }
            } else {
                attribute.setApprovedUsage(ApprovedUsage.MODEL_ALLINSIGHTS);
                attribute.setCategory(Category.ACCOUNT_INFORMATION);
                attribute.setDisplayDiscretizationStrategy("{\"unified\": {}}");
                attribute.setTags(ModelingMetadata.INTERNAL_TAG);
                setDisplayNameAndOthers(attribute, name);
            }
        }
    }

    private static void setDisplayNameAndOthers(Attribute attribute, String name) {
        if (name.matches("Product_.*_Revenue")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Revenue");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Last Period Spend for " + matcher.group(1));
            attribute.setDescription("Product spend in last period");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches("Product_.*_RevenueRollingSum6")) {
            Pattern pattern = Pattern.compile("Product_(.*)_RevenueRollingSum6");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("6-Period Spend for " + matcher.group(1));
            attribute.setDescription("Product spend for last 6 periods");
            attribute.setFundamentalType(FundamentalType.CURRENCY);
            return;
        }
        if (name.matches("Product_.*_RevenueMomentum3")) {
            Pattern pattern = Pattern.compile("Product_(.*)_RevenueMomentum3");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Rate of Change of 3-Period Spend for " + matcher.group(1));
            attribute.setDescription(
                    "Percent change in the 3 period spend, where values > 0 show increasing spend and < 0 indicate decreasing spend");
            return;
        }
        if (name.matches("Product_.*_Units")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Units");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Last Period Units for " + matcher.group(1));
            attribute.setDescription("Units purchased in last period");
            return;
        }
        if (name.matches("Product_.*_Span")) {
            Pattern pattern = Pattern.compile("Product_(.*)_Span");
            Matcher matcher = pattern.matcher(name);
            matcher.matches();
            attribute.setDisplayName("Purchase Recency for " + matcher.group(1));
            attribute.setDescription(
                    "Indicator of how recently a customer purchased, where higher values indicate more recent purchase (e.g. 1 = Last Period  and 0 = Never)");
            return;
        }
    }
}
