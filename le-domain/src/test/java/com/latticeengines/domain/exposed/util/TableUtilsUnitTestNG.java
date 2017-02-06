package com.latticeengines.domain.exposed.util;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;

public class TableUtilsUnitTestNG {

    private Schema schema;

    @Test(groups = "unit")
    public void testTableUtilsPreservingColumnOrdering() {
        createSchema();
        assertOrderingIsRight();
    }

    private void createSchema() {
        Table table = new Table();
        table.setName("table");
        Attribute idAttr = new Attribute();
        idAttr.setName(InterfaceName.Id.toString());
        idAttr.setDisplayName(InterfaceName.Id.toString());
        idAttr.setSourceLogicalDataType("");
        idAttr.setPhysicalDataType(Type.STRING.name());
        Attribute modelIdAttr = new Attribute();
        modelIdAttr.setName("ModelId");
        modelIdAttr.setDisplayName("ModelId");
        modelIdAttr.setSourceLogicalDataType("");
        modelIdAttr.setPhysicalDataType(Type.STRING.name());
        Attribute scoreAttr = new Attribute();
        scoreAttr.setName("Score");
        scoreAttr.setDisplayName("Score");
        scoreAttr.setSourceLogicalDataType("");
        scoreAttr.setPhysicalDataType(Type.DOUBLE.name());
        table.addAttribute(idAttr);
        table.addAttribute(modelIdAttr);
        table.addAttribute(scoreAttr);
        Attribute attr = new Attribute();
        attr.setName("ExtraAttribute");
        attr.setDisplayName("ExtraAttribute");
        attr.setSourceLogicalDataType("");
        attr.setPhysicalDataType(Type.ENUM.name());
        table.addAttribute(attr);
        schema = TableUtils.createSchema(table.getName(), table);
    }

    private void assertOrderingIsRight() {
        List<Schema.Field> fields = schema.getFields();
        Assert.assertEquals(fields.get(0).name(), InterfaceName.Id.toString());
        Assert.assertEquals(fields.get(1).name(), "ModelId");
        Assert.assertEquals(fields.get(2).name(), "Score");
        Assert.assertEquals(fields.get(3).name(), "ExtraAttribute");
    }
}
