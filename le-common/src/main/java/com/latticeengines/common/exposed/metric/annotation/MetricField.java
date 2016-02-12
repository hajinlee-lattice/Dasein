package com.latticeengines.common.exposed.metric.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MetricField {

    String name();
    FieldType fieldType() default FieldType.STRING;

    enum FieldType {
        STRING(String.class), DOUBLE(Double.class), INTEGER(Integer.class), BOOLEAN(Boolean.class);

        private Class<?> javaClass;

        FieldType(Class<?> javaClass) {
            this.javaClass = javaClass;
        }

        public Class<?> getJavaClass() {
            return javaClass;
        }
    }

}
