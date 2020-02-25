package com.latticeengines.domain.exposed.serviceapps.core;

public class ValidationMsg {

    public static class Errors {
        public static final String FORBID_CUSTOMIZATION = "Property %s does not allow customization.";
        public static final String FORBID_SET_INACTIVE = "User cannot change attribute %s to inactive.";
        public static final String FORBID_SET_ACTIVE = "User cannot change deprecated attribute %s to active.";
        public static final String INACTIVE_USAGE = "Inactive attribute: %s Usage group %s should be false.";
        public static final String INVALID_ATTRIBUTE_USAGE_CHANGE = "Usage change is not allowed(Usage group %s ,"
                + "Attribute type %s)";
        public static final String EXCEED_LIMIT = "You are trying to enable %d %s attributes, Please not choose more than the limit %d";
        public static final String EXCEED_USAGE_LIMIT = "You are trying to enable %d %s attributes, Please not choose more than the limit %d";
        public static final String IMPACT_SEGMENTS = "Attribute %s change will impact the following segments: [%s]";
        public static final String IMPACT_RATING_ENGINES = "Attribute %s change will impact the following rating "
                + "engines: [%s]";
        public static final String IMPACT_RATING_MODELS = "Attribute %s change will impact the following rating "
                + "models: [%s]";
        public static final String IMPACT_PLAYS = "Attribute %s change will impact the following plays: [%s]";
        public static final String DUPLICATED_NAME = "The name already exists";
    }
}
