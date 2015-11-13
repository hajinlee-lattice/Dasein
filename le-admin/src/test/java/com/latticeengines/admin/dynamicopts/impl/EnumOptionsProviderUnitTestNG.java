package com.latticeengines.admin.dynamicopts.impl;

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.dynamicopts.OptionsProvider;

public class EnumOptionsProviderUnitTestNG {

    @Test(groups = "unit")
    public void testEnumOptionsProvider() {
        OptionsProvider provider = new EnumOptionsProvider(MyEnum.class);
        List<String> options = provider.getOptions();
        Assert.assertEquals(options.size(), MyEnum.values().length);
        Assert.assertTrue(options.contains(MyEnum.OPTION1.toString()));
        Assert.assertTrue(options.contains(MyEnum.OPTION2.toString()));
        Assert.assertTrue(options.contains(MyEnum.OPTION3.toString()));
    }

    private enum MyEnum {
        OPTION1("Option1"), OPTION2("Option2"), OPTION3("Option3");

        private String name;

        MyEnum(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return this.name;
        }
    }
}
