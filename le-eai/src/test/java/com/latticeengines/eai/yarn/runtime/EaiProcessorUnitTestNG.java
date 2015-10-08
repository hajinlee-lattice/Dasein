package com.latticeengines.eai.yarn.runtime;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.eai.ImportConfiguration;

public class EaiProcessorUnitTestNG {

    @Test(groups = "unit")
    public void instantiate() {
        EaiProcessor processor = new EaiProcessor();
        Class<?> c = processor.getType();
        assertEquals(c, ImportConfiguration.class);
    }
}
