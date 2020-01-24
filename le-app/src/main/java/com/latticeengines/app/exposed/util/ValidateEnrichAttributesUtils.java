package com.latticeengines.app.exposed.util;

public final class ValidateEnrichAttributesUtils {

    protected ValidateEnrichAttributesUtils() {
        throw new UnsupportedOperationException();
    }

    public static final int MIN_PREMIUM_ENRICHMENT_ATTRIBUTES = 0;
    public static final int DEFAULT_PREMIUM_ENRICHMENT_ATTRIBUTES = 32;

    public static int validateEnrichAttributes(String maxPremiumEnrichAttributesStr) {
        maxPremiumEnrichAttributesStr = maxPremiumEnrichAttributesStr.replaceAll("\"", "");
        int premiumEnrichAttributes = Integer.parseInt(maxPremiumEnrichAttributesStr);
        if (premiumEnrichAttributes < MIN_PREMIUM_ENRICHMENT_ATTRIBUTES) {
            throw new RuntimeException(String.format("PremiumEnrichAttributes: %d is less than %d.",
                    premiumEnrichAttributes, MIN_PREMIUM_ENRICHMENT_ATTRIBUTES));
        }
        return premiumEnrichAttributes;
    }

}
