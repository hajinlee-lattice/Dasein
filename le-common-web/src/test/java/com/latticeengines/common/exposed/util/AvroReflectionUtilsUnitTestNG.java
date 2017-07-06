package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.Nullable;
import org.testng.annotations.Test;

public class AvroReflectionUtilsUnitTestNG {
    public static class SimpleClass {
        public enum MyEnum {
            ONE, TWO, BOB
        }

        private String stringField = "stringField";
        private int integerField = 1;
        @Nullable
        private Long longField = null;
        private MyEnum myEnum = MyEnum.BOB;
        private List<SimpleClass> list = new ArrayList<>();

        public String getStringField() {
            return stringField;
        }

        public void setStringField(String stringField) {
            this.stringField = stringField;
        }

        public int getIntegerField() {
            return integerField;
        }

        public void setIntegerField(int integerField) {
            this.integerField = integerField;
        }

        public long getLongField() {
            return longField;
        }

        public void setLongField(long longField) {
            this.longField = longField;
        }

        public void add(SimpleClass simpleClass) {
            list.add(simpleClass);
        }

        public MyEnum getMyEnum() {
            return myEnum;
        }

        public void setMyEnum(MyEnum myEnum) {
            this.myEnum = myEnum;
        }

        public List<SimpleClass> getList() {
            return list;
        }

        public void setList(List<SimpleClass> list) {
            this.list = list;
        }
    }

    @Test(groups = "unit")
    public void testDeSer() {
        SimpleClass original = new SimpleClass();
        SimpleClass child1 = new SimpleClass();
        original.add(child1);
        SimpleClass child2 = new SimpleClass();
        original.add(child2);

        GenericRecord record = AvroReflectionUtils.toGenericRecord(original, SimpleClass.class);
        SimpleClass reconstituted = AvroReflectionUtils.fromGenericRecord(record);
        assertEquals(reconstituted.getStringField(), original.getStringField());
    }
}
