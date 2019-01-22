package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer;

import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.modeling.SamplingType;

public class SamplingJobCustomizerFactoryUnitTestNG {
    private SamplingType samplingType;

    private SamplingJobCustomizerFactory samplingJobCustomizerFactory;

    @BeforeClass(groups = "unit")
    public void setup() {
        samplingJobCustomizerFactory = new SamplingJobCustomizerFactory();
    }

    @Test(groups = "unit")
    public void testGetDefaultSamplingJobCustomizer() throws ClassNotFoundException {
        samplingType = SamplingType.DEFAULT_SAMPLING;
        SamplingJobCustomizer samplingJobCustomizer = samplingJobCustomizerFactory.getCustomizer(samplingType);
        assertTrue(samplingJobCustomizer instanceof DefaultSamplingJobCustomizer);
    }

    @Test(groups = "unit")
    public void testGetStratifiedSamplingJobCustomizer() throws ClassNotFoundException {
        samplingType = SamplingType.STRATIFIED_SAMPLING;
        SamplingJobCustomizer samplingJobCustomizer = samplingJobCustomizerFactory.getCustomizer(samplingType);
        assertTrue(samplingJobCustomizer instanceof StratifiedSamplingJobCustomizer);
    }
}
