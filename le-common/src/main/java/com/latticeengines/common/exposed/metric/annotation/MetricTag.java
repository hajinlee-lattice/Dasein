package com.latticeengines.common.exposed.metric.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MetricTag {

    Tag tag();

    enum Tag {
        ENVIRONMENT("Environment"), ARTIFACT_VERSION("ArtifactVersion"), TENANT_ID("TenantId"), LDC_SOURCE_NAME(
                "SourceName");

        private static List<Tag> providedTags;

        static {
            Set<String> names = new HashSet<>();
            for (Tag tag : Tag.values()) {
                if (names.contains(tag.getTagName())) {
                    throw new RuntimeException("Duplicated MetricTag [" + tag.getTagName() + "]");
                } else {
                    names.add(tag.getTagName());
                }
            }

            providedTags = Arrays.asList(ENVIRONMENT, ARTIFACT_VERSION);

        }

        private final String name;

        Tag(String name) {
            this.name = name;
        }

        public String getTagName() {
            return name;
        }

        public static Collection<Tag> providedByFramework() {
            return providedTags;
        }
    }

}
