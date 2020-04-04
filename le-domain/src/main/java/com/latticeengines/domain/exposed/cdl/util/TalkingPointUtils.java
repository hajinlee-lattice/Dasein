package com.latticeengines.domain.exposed.cdl.util;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.query.AttributeLookup;

public final class TalkingPointUtils {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointUtils.class);

    private TalkingPointUtils() { }

    public static Set<AttributeLookup> extractAttributes(String content) {
        Pattern pattern = Pattern.compile("\\{!([A-Za-z0-9_]+)\\.([A-Za-z0-9_]+)}");
        Matcher matcher = pattern.matcher(content);

        Set<AttributeLookup> attributes = new HashSet<>();
        while (matcher.find()) {
            String attributeLookupStr = String.format("%s.%s", matcher.group(1), matcher.group(2));
            AttributeLookup attributeLookup = AttributeLookup.fromString(attributeLookupStr);
            if (attributeLookup != null) {
                attributes.add(attributeLookup);
            }
        }
        return attributes;
    }
}
