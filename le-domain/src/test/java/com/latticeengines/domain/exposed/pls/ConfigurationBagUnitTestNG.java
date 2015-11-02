package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ConfigurationBagUnitTestNG {
    private ConfigurationBag bag;

    public class TestOption implements HasOptionAndValue {
        private String option;
        private String value;

        public TestOption(String option, String value) {
            this.option = option;
            this.value = value;
        }

        @Override
        public String getOption() {
            return option;
        }

        @Override
        public void setOption(String option) {
            this.option = option;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public void setValue(String value) {
            this.value = value;
        }
    }

    @BeforeClass(groups = "unit")
    @SuppressWarnings("unchecked")
    public void setup() {
        List<TestOption> options = new ArrayList<>();
        TestOption option = new TestOption("testString", "foo");
        options.add(option);
        option = new TestOption("testInt", "123");
        options.add(option);
        option = new TestOption("testDouble", "123.123");
        options.add(option);

        bag = new ConfigurationBag(List.class.cast(options));
    }

    @Test(groups = "unit")
    public void testGetInt() {
        int value = bag.getInt("testInt", 0);
        Assert.assertEquals(value, 123);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testGetInvalidInt() {
        bag.getInt("testDouble", 123);
    }
}
