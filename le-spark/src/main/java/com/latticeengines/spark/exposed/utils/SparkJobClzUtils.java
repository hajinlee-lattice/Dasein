package com.latticeengines.spark.exposed.utils;

import org.reflections.Reflections;

import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;

public final class SparkJobClzUtils {

    private static final String BASE_PKG = "com.latticeengines.spark.exposed.job";

    protected SparkJobClzUtils() {
        throw new UnsupportedOperationException();
    }

    public static <C extends SparkJobConfig> Class<? extends AbstractSparkJob<C>>
    findSparkJobClz(String jobClzName, Class<C> configClz) throws ClassNotFoundException {
        Reflections reflections = new Reflections(BASE_PKG);
        for (Class<?> clz: reflections.getSubTypesOf(AbstractSparkJob.class)) {
            if (clz.getSimpleName().equals(jobClzName)) {
                //noinspection unchecked
                return (Class<? extends AbstractSparkJob<C>>) clz;
            }
        }
        throw new ClassNotFoundException("Cannot find a spark job class named " + jobClzName);
    }

}
