package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import java.lang.reflect.Constructor;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.SamplingType;

@Component("samplingJobCustomizerFactory")
public class SamplingJobCustomizerFactory {

    public SamplingJobCustomizer getCustomizer(SamplingType samplingType) {
        String customizerClassName = samplingType.getCustomizerClassName();
        Class<?> customizerClass = null;
        try {
            customizerClass = Class.forName(customizerClassName);
            Constructor<?> costructor = customizerClass.getConstructor();
            return (SamplingJobCustomizer) costructor.newInstance();
        } catch (Exception ex) {
            throw new LedpException(LedpCode.LEDP_15011, ex);
        }
    }
}
