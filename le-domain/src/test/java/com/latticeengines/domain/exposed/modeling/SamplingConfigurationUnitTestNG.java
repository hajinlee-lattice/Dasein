package com.latticeengines.domain.exposed.modeling;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class SamplingConfigurationUnitTestNG {

    private SamplingConfiguration config;

    @BeforeClass(groups = "unit")
    public void setup() {
        config = new SamplingConfiguration();
        config.setCustomer("DELL");
        config.setTable("DELL_EVENT_TABLE_TEST");
        config.setTrainingPercentage(80);
        config.setTrainingSetCount(3);
        config.setProperty(SamplingProperty.TRAINING_DATA_SIZE.name(), "8000");
        config.setProperty(SamplingProperty.TRAINING_SET_SIZE.name(), "2667");
        config.setSamplingType(SamplingType.BOOTSTRAP_SAMPLING);
        config.setTrainingElements();
    }

    @Test(groups = "unit")
    public void testSerializeDeserialize() {
        String jsonString = config.toString();
        SamplingConfiguration deserializedConfig = JsonUtils.deserialize(jsonString, SamplingConfiguration.class);

        assertEquals(deserializedConfig.getProperty(SamplingProperty.TRAINING_SET_SIZE.name()), "2667");
        assertEquals(deserializedConfig.getProperties().size(), 2);
        assertEquals(deserializedConfig.getSamplingType(), SamplingType.BOOTSTRAP_SAMPLING);
        assertEquals(deserializedConfig.getSamplingElements().size(), 5);
        assertEquals(deserializedConfig.toString(), jsonString);
    }

    @Test(groups = "unit")
    public void testSetTrainingPercentage() {
        config.setTrainingPercentage(60);
        assertEquals(config.getTestPercentage(), 40);
        config.setTrainingPercentage(80);
        assertEquals(config.getTestPercentage(), 20);
        Exception ex = null;
        try {
            config.setTrainingPercentage(0);
        } catch (Exception ex2) {
            ex = ex2;
        }
        checkIsLedpException(ex);
    }

    @Test(groups = "unit")
    public void testSetTrainingSetCount() {
        config.setTrainingSetCount(5);
        config.setTrainingElements();
        assertEquals(config.getTrainingElements().size(), 5);
        assertEquals(config.getSamplingElements().size(), 7);
    }

    @Test(groups = "unit")
    public void testSetSamplingRate() {
        Exception ex = null;
        try {
            config.setSamplingRate(500);
        } catch (Exception ex2) {
            ex = ex2;
        }
        checkIsLedpException(ex);
    }

    private void checkIsLedpException(Exception ex) {
        assertTrue(ex instanceof LedpException);
        LedpException ledpException = (LedpException) ex;
        assertEquals(ledpException.getCode(), LedpCode.LEDP_15012);
    }
}
