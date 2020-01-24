package com.latticeengines.monitor.exposed.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.latticeengines.monitor.exposed.metrics.impl.InstrumentRegistry;

@Repeatable(InvocationMeters.class)
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface InvocationMeter {

    String name(); // value of the name tag
    String instrument() default InstrumentRegistry.DEFAULT; // name of the instrument implementation
    String measurment() default ""; // split into separate measurement

    // track errors in a separate metrics stream
    boolean errors() default false;

}
