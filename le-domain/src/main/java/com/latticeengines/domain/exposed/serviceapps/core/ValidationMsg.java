package com.latticeengines.domain.exposed.serviceapps.core;

public class ValidationMsg {

    public static class Warnings {

    }

    public static class Errors {
        public static final String FORBID_CUSTOMIZATION = "Property %s does not allow customization.";
        public static final String FORBID_SET_INACTIVE = "User cannot change attribute %s to inactive.";
        public static final String FORBID_SET_ACTIVE = "User cannot change deprecated attribute %s to active.";
        public static final String UPDATE_INACTIVE = "Cannot change usage %s for inactive attribute: %s";
        public static final String INACTIVE_USAGE = "Inactive attribute: %s Usage group %s should be false.";
        public static final String INVALID_ATTRIBUTE_USAGE_CHANGE = "Usage change is not allowed(Usage group %s ," +
                "Attribute type %s)";
    }
}
