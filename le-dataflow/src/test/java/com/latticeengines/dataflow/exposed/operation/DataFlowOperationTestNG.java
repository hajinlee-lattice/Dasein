package com.latticeengines.dataflow.exposed.operation;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.query.ReferenceInterpretation;
import com.latticeengines.common.exposed.query.SingleReferenceLookup;
import com.latticeengines.common.exposed.query.Sort;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.functionalframework.DataFlowOperationFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;

public class DataFlowOperationTestNG extends DataFlowOperationFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testSort() {
        execute(new TypesafeDataFlowBuilder<DataFlowParameters>() {
            @Override
            public Node construct(DataFlowParameters parameters) {
                Node lead = addSource("Lead");
                List<SingleReferenceLookup> lookups = new ArrayList<>();
                lookups.add(new SingleReferenceLookup("Email", ReferenceInterpretation.COLUMN));
                Sort sort = new Sort(lookups);
                return lead.sort(sort);
            }
        });

        List<GenericRecord> output = readOutput();
        String lastEmail = null;
        for (GenericRecord record : output) {
            String email = record.get("Email").toString();
            if (lastEmail != null) {
                Assert.assertTrue(email.compareTo(lastEmail) >= 0);
            }
            lastEmail = email;
        }
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

}
