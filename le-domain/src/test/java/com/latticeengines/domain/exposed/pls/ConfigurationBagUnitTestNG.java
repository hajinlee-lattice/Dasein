package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ConfigurationBagUnitTestNG {
    public enum OptionName {
        testInt, //
        testString, //
        testDouble, //
        testNotExists
    }

    private ConfigurationBag<TestOption, OptionName> bag;

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

    @BeforeMethod(groups = "unit")
    @SuppressWarnings("unchecked")
    public void setup() {
        List<TestOption> options = new ArrayList<>();
        TestOption option = new TestOption(OptionName.testString.toString(), "foo");
        options.add(option);
        option = new TestOption(OptionName.testInt.toString(), "123");
        options.add(option);
        option = new TestOption(OptionName.testDouble.toString(), "123.123");
        options.add(option);

        bag = new ConfigurationBag<TestOption, OptionName>(List.class.cast(options));
    }

    @Test(groups = "unit")
    public void testGetInt() {
        int value = bag.getInt(OptionName.testInt, 0);
        Assert.assertEquals(value, 123);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testGetInvalidInt() {
        bag.getInt(OptionName.testDouble, 123);
    }

    @Test(groups = "unit")
    public void testSetNullValue() {
        bag.set(OptionName.testInt, null);
        Integer value = bag.get(OptionName.testInt, Integer.class);
        Assert.assertEquals(value, null);
    }

    @Test(groups = "unit", expectedExceptions = RuntimeException.class)
    public void testValueDoesntExist() {
        bag.get(OptionName.testNotExists, Integer.class);
    }
}
