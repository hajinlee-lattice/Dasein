package com.latticeengines.dataflow.runtime.cascading;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

public class GroupAndExpandFieldsBufferUnitTestNG {

    @Test(groups = "unit")
    public void operate() {
        GroupAndExpandFieldsBuffer buffer = new GroupAndExpandFieldsBuffer(3, "Category",
                Arrays.asList("ML_%s_Topic", "ML_%s_User"), new Fields("DomainID", "ML_Category1_Topic",
                        "ML_Category1_User", "ML_Category2_Topic", "ML_Category2_User"));
        BufferCall<?> bufferCall = mock(BufferCall.class);

        Fields fields = new Fields("DomainID");
        Tuple tuple = new Tuple();
        tuple.add(1);
        TupleEntry group = new TupleEntry(fields, tuple);
        when(bufferCall.getGroup()).thenReturn(group);

        fields = new Fields("Category", "ML_Category_Topic", "ML_Category_User");
        tuple = new Tuple();
        tuple.add("category1");
        tuple.add("topic1");
        tuple.add("user1");

        TupleEntry arg = new TupleEntry(fields, tuple);
        List<TupleEntry> entries = new ArrayList<>();
        entries.add(arg);

        tuple = new Tuple();
        tuple.add("CAtegory2");
        tuple.add("topic2");
        tuple.add("user2");

        arg = new TupleEntry(fields, tuple);
        entries.add(arg);
        Iterator<TupleEntry> iterator = entries.iterator();
        when(bufferCall.getArgumentsIterator()).thenReturn(iterator);

        TupleEntryCollector collector = mock(TupleEntryCollector.class);
        when(bufferCall.getOutputCollector()).thenReturn(collector);

        final List<Tuple> results = new ArrayList<Tuple>();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Object[] params = invocation.getArguments();
                results.add((Tuple) params[0]);
                return results;
            }
        }).when(collector).add((Tuple) any());

        buffer.operate(null, bufferCall);

        Assert.assertEquals(results.size(), 1);
        Assert.assertEquals(results.get(0).getObject(0), 1);
        Assert.assertEquals(results.get(0).getObject(1), "topic1");
        Assert.assertEquals(results.get(0).getObject(2), "user1");
        Assert.assertEquals(results.get(0).getObject(3), "topic2");
        Assert.assertEquals(results.get(0).getObject(4), "user2");
    }
}
