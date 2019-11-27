package com.latticeengines.domain.exposed.util;

import static com.latticeengines.domain.exposed.util.ImportWorkflowSpecUtils.convertFieldNameToAvroFriendlyFormat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;

public class ImportWorkflowSpecUtilsUnitTestNG {

    @Test(groups = "unit")
    public void testConvertFieldNameToAvroFriendlyFormat() {
        String malformedName = "2name?*wer23";
        String expectedString = "avro_2name__wer23";
        assertEquals(convertFieldNameToAvroFriendlyFormat(malformedName), expectedString);

        Table table = new Table();
        Attribute duplicateAttribute1 = new Attribute();
        duplicateAttribute1.setName(convertFieldNameToAvroFriendlyFormat("1-200"));
        Attribute duplicateAttribute2 = new Attribute();
        duplicateAttribute2.setName(convertFieldNameToAvroFriendlyFormat("avro_1_200"));
        Attribute duplicateAttribute3 = new Attribute();
        duplicateAttribute3.setName(convertFieldNameToAvroFriendlyFormat("1_200"));
        table.addAttribute(duplicateAttribute1);
        table.addAttribute(duplicateAttribute2);
        table.addAttribute(duplicateAttribute3);
        table.deduplicateAttributeNames();

        final List<String> expectedNames = new ArrayList<String>();
        expectedNames.add("avro_1_200");
        expectedNames.add("avro_1_200_1");
        expectedNames.add("avro_1_200_2");
        boolean allExpectedNamesAreAvroFriendly = Iterables.all(expectedNames, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return AvroUtils.isAvroFriendlyFieldName(input);
            }
        });
        assertTrue(allExpectedNamesAreAvroFriendly);

        boolean convertAndDedupeAreRight = Iterables.any(table.getAttributes(), new Predicate<Attribute>() {
            @Override
            public boolean apply(Attribute input) {
                return expectedNames.contains(input.getName());
            }
        });
        assertTrue(convertAndDedupeAreRight);
    }
}
