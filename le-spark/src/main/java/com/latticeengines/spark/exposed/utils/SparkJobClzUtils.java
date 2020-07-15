package com.latticeengines.spark.exposed.utils;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.reflections.Reflections;

import com.latticeengines.domain.exposed.cdl.activity.EmptyStreamException;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;

public final class SparkJobClzUtils {

    private static final String BASE_PKG = "com.latticeengines.spark.exposed.job";
    private static final Set<Class<? extends Throwable>> NOT_RETRYABLE_ERRS = Collections
            .singleton(EmptyStreamException.class);

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

    public static boolean isExceptionRetryable(Exception exception) {
        if (exception == null) {
            return true;
        }

        // seems like exception in scala job is serialized into message
        boolean noClzInMessages = ExceptionUtils.getThrowableList(exception) //
                .stream() //
                .noneMatch(t -> NOT_RETRYABLE_ERRS //
                        .stream() //
                        .anyMatch(ex -> t.getMessage().contains(ex.getSimpleName())));

        boolean noClzInChain = NOT_RETRYABLE_ERRS.stream()
                .noneMatch(ex -> ExceptionUtils.indexOfType(exception, ex) != -1);
        return noClzInChain && noClzInMessages;
    }

}
