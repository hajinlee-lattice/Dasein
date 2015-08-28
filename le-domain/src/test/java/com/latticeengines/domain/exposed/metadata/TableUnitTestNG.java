package com.latticeengines.domain.exposed.metadata;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class TableUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        Table table = new Table();
        table.setName("source");
        Extract e1 = new Extract();
        e1.setName("e1");
        e1.setPath("/extract1");
        Extract e2 = new Extract();
        e2.setName("e2");
        e2.setPath("/extract2");
        Extract e3 = new Extract();
        e3.setName("e3");
        e3.setPath("/extract3");
        table.addExtract(e1);
        table.addExtract(e2);
        table.addExtract(e3);
        PrimaryKey pk = new PrimaryKey();
        Attribute pkAttr = new Attribute();
        pkAttr.setName("ID");
        pk.addAttribute(pkAttr);
        table.setPrimaryKey(pk);

        String serializedStr = JsonUtils.serialize(table);

        Table deserializedTable = JsonUtils.deserialize(serializedStr, Table.class);

        assertEquals(deserializedTable.getName(), table.getName());
        assertEquals(deserializedTable.getAttributes().size(), table.getAttributes().size());
        assertEquals(deserializedTable.getPrimaryKey().getAttributeNames()[0], //
                table.getPrimaryKey().getAttributeNames()[0]);
    }
}
