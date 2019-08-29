package com.latticeengines.domain.exposed.query;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EntityTypeUtils {

    private static final String FEEDTYPE_PATTERN = "([A-Za-z0-9_()\\[\\]]+)_(%s)";

    public static EntityType matchFeedType(String feedType) {
        for (EntityType entityType : EntityType.values()) {
            if (entityType.getDefaultFeedTypeName().equalsIgnoreCase(feedType)) {
                return entityType;
            }
            Pattern feedTypePattern = Pattern.compile(String.format(FEEDTYPE_PATTERN,
                    entityType.getDefaultFeedTypeName()));
            Matcher matcher = feedTypePattern.matcher(feedType);
            if (matcher.find()) {
                return entityType;
            }
        }
        return null;
    }
}
