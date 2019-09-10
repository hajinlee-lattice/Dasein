package com.latticeengines.domain.exposed.query;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

public class EntityTypeUtils {

    private static final String FEEDTYPE_PATTERN = "([A-Za-z0-9_()\\[\\]]+)_(%s)";
    private static final String FULL_FEEDTYPE = "%s_%s";

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

    public static String getSystemName(String feedType) {
        List<EntityType> allTypes = Arrays.asList(EntityType.values());
        Pattern feedTypePattern = Pattern.compile(String.format(FEEDTYPE_PATTERN,
                allTypes.stream().map(EntityType::getDefaultFeedTypeName).collect(Collectors.joining("|"))));
        Matcher matcher = feedTypePattern.matcher(feedType);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return StringUtils.EMPTY;
    }

    public static String generateFullFeedType(String systemName, EntityType entityType) {
        Preconditions.checkNotNull(systemName);
        Preconditions.checkNotNull(entityType);
        return String.format(FULL_FEEDTYPE, systemName, entityType.getDefaultFeedTypeName());
    }
}
