package com.latticeengines.common.exposed.metric.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MetricTagGroup {

    String[]includes() default {};

    String[]excludes() default {};

}
