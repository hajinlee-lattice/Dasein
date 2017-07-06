package com.latticeengines.dataflow.exposed.operation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.query.ColumnLookup;
import com.latticeengines.domain.exposed.query.Lookup;
import com.latticeengines.domain.exposed.query.Sort;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;

public class DataFlowOperationTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testSort() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                List<Lookup> lookups = new ArrayList<>();
                lookups.add(new ColumnLookup("Email"));
                Sort sort = new Sort(lookups);
                return lead.sort(sort);
            }
        });

        List<GenericRecord> output = readOutput();
        String lastEmail = null;
        if (output.size() == 0) {
            throw new RuntimeException("Record is empty");
        }
        for (GenericRecord record : output) {
            String email = record.get("Email").toString();
            if (StringUtils.isBlank(email)) {
                Assert.assertTrue(StringUtils.isBlank(lastEmail),
                        "If this email is blan, last email should also be blank, but it is " + lastEmail + " instead.");
            } else {
                Assert.assertTrue(StringUtils.isBlank(lastEmail) || email.compareTo(lastEmail) >= 0, email
                        + " should not be after " + lastEmail);
            }
            lastEmail = email;
        }

        Schema schema = output.get(0).getSchema();
        Assert.assertTrue(schema.getFields().get(0).name().equals("Id"));
        Assert.assertTrue(schema.getFields().get(1).name().equals("LastName"));
        Assert.assertTrue(schema.getFields().get(2).name().equals("FirstName"));
        Assert.assertTrue(schema.getFields().get(schema.getFields().size() - 1).name().equals("LastModifiedDate"));
    }

    @Test(groups = "functional")
    public void testStopListAllFilter() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                Node lead2 = addSource("Lead").renamePipe("Lead2");
                return lead.stopList(lead2, "Id", "Id");
            }
        });

        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), 0);
    }

    @Test(groups = "functional")
    public void testStopListAllPass() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                Node contact = addSource("Contact");
                return lead.stopList(contact, "Id", "Id");
            }
        });

        List<GenericRecord> input = readInput("Lead");
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), input.size());
    }

    @Test(groups = "functional")
    public void testStopListAllPassBothLeftAndRightShareColumn() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                Node contact = addSource("Contact");
                return lead.stopList(contact, "Id", "Email");
            }
        });

        List<GenericRecord> input = readInput("Lead");
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), input.size());
    }

    @Test(groups = "functional")
    public void testGroupByAndLimit() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                return lead.groupByAndLimit(new FieldList("Email"), 1);
            }
        });

        List<GenericRecord> input = readInput("Lead");
        List<GenericRecord> output = readOutput();
        Assert.assertNotEquals(input.size(), output.size());
        final Map<Object, Integer> histogram = histogram(output, "Email");
        Assert.assertTrue(Iterables.all(histogram.keySet(), new Predicate<Object>() {

            @Override
            public boolean apply(@Nullable Object input) {
                return histogram.get(input) == 1;
            }
        }));
    }

    @Test(groups = "functional")
    public void testRename() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                return lead.rename(new FieldList("Email"), new FieldList("Foo"));
            }
        });

        Schema schema = getOutputSchema();
        Assert.assertNotNull(schema.getField("Foo"));
        Assert.assertNull(schema.getField("Email"));
    }

    @Test(groups = "functional")
    public void testTransformCompanyNameLength() throws Exception {
        final TransformDefinition definition = TransformationPipeline.stdLengthCompanyName;
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead2");
                return lead.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
            }
        });
        List<GenericRecord> output = readOutput();
        for (GenericRecord record : output) {
            Assert.assertNotNull(record.get(definition.output));
        }
    }

    @Test(groups = "functional")
    public void testTransformFundingStage() throws Exception {
        final TransformDefinition definition = TransformationPipeline.stdVisidbDsPdFundingstageOrdered;
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead2");
                return lead.addTransformFunction("com.latticeengines.transform.v2_0_25.functions", definition);
            }
        });
        List<GenericRecord> output = readOutput();
        Assert.assertEquals(output.size(), 4826);
    }

}
