package com.latticeengines.pls.util;

public class ValidateEnrichAttributesUtils {

    public static final int MIN_PREMIUM_ENRICHMENT_ATTRIBUTES = 0;
    public static final int DEFAULT_PREMIUM_ENRICHMENT_ATTRIBUTES = 10;
    public static final int MAX_PREMIUM_ENRICHMENT_ATTRIBUTES = 1000;

    public static int validateEnrichAttributes(String maxPremiumEnrichAttributesStr) {
        int premiumEnrichAttributes = Integer.parseInt(maxPremiumEnrichAttributesStr);
        if (premiumEnrichAttributes < MIN_PREMIUM_ENRICHMENT_ATTRIBUTES
                || premiumEnrichAttributes > MAX_PREMIUM_ENRICHMENT_ATTRIBUTES) {
            throw new RuntimeException(String.format("PremiumEnrichAttributes: %d is out of the range of %d and %d.",
                    premiumEnrichAttributes, MIN_PREMIUM_ENRICHMENT_ATTRIBUTES, MAX_PREMIUM_ENRICHMENT_ATTRIBUTES));
        }
        return premiumEnrichAttributes;
    }

}
