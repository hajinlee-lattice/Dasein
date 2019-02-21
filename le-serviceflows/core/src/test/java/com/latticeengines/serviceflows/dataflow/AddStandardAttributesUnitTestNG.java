package com.latticeengines.serviceflows.dataflow;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.transform.TransformationPipeline;

public class AddStandardAttributesUnitTestNG {

    private String identifier;

    private Table t;

    private DataFlowContext context;

    @BeforeTest(groups = "unit")
    public void setup() {
        identifier = "sourcetable";
        t = new Table();
        Attribute attr1 = new Attribute();
        attr1.setName(TransformationPipeline.stdLengthDomain.output);
        attr1.setDisplayName(TransformationPipeline.stdLengthDomain.outputDisplayName);
        t.addAttribute(attr1);
        Map<String, Table> map = new HashMap<>();
        map.put(identifier, t);
        context = new DataFlowContext();
        context.setProperty(DataFlowProperty.SOURCETABLES, map);
    }

    @Test(groups = "unit")
    public void resolveDuplicateName() throws Exception {
        AddStandardAttributes addStandardAttributes = new AddStandardAttributes();
        addStandardAttributes.setDataFlowCtx(context);

        @SuppressWarnings("unchecked")
        Class<Node> c = (Class<Node>) Class.forName(Node.class.getCanonicalName());
        Constructor<Node> ctor = c.getDeclaredConstructor(String.class, CascadingDataFlowBuilder.class);
        ctor.setAccessible(true);
        Node eventTable = ctor.newInstance(identifier, addStandardAttributes);

        addStandardAttributes.resolveDuplicateName(eventTable, TransformationPipeline.stdLengthDomain);

        assertEquals(TransformationPipeline.stdLengthDomain.output, "Domain_Length_1");
        assertEquals(TransformationPipeline.stdLengthDomain.outputDisplayName, "Length of Domain Name 1");
    }
}
