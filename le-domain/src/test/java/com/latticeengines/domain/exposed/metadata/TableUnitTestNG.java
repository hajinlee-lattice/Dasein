package com.latticeengines.domain.exposed.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.avro.Schema;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.security.Tenant;

public class TableUnitTestNG {

    private Table table = null;

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        table = createTable();
    }

    @Test(groups = "unit")
    public void testSerDe() {
        String serializedStr = JsonUtils.serialize(table);

        Table deserializedTable = JsonUtils.deserialize(serializedStr, Table.class);

        assertEquals(deserializedTable.getName(), table.getName());
        assertEquals(deserializedTable.getAttributes().size(), table.getAttributes().size());
        assertEquals(deserializedTable.getPrimaryKey().getAttributeNames()[0], //
                table.getPrimaryKey().getAttributeNames()[0]);
    }

    @Test(groups = "unit")
    public void getModelingMetadata() {
        ModelingMetadata metadata = table.getModelingMetadata();
        assertEquals(metadata.getAttributeMetadata().size(), 4);
    }

    @Test(groups = "unit")
    public void getAttribute() {
        Attribute unknown = table.getAttribute("foo");
        assertTrue(unknown == null);
    }

    private Table createTable() {
        Tenant tenant = new Tenant();
        tenant.setId("T1");
        tenant.setName("Tenant1");
        Table table = new Table();
        table.setTenant(tenant);
        table.setName("Account");
        table.setDisplayName("Account");
        Extract e1 = createExtract("e1");
        Extract e2 = createExtract("e2");
        Extract e3 = createExtract("e3");
        table.addExtract(e1);
        table.addExtract(e2);
        table.addExtract(e3);
        PrimaryKey pk = createPrimaryKey();
        LastModifiedKey lk = createLastModifiedKey();
        table.setPrimaryKey(pk);
        table.setLastModifiedKey(lk);

        Attribute pkAttr = new Attribute();
        pkAttr.setName("ID");
        pkAttr.setDisplayName("Id");
        pkAttr.setLength(10);
        pkAttr.setPrecision(10);
        pkAttr.setScale(10);
        pkAttr.setPhysicalDataType(Schema.Type.INT.toString());
        pkAttr.setSourceLogicalDataType("Identity");
        pkAttr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);

        Attribute lkAttr = new Attribute();
        lkAttr.setName("LID");
        lkAttr.setDisplayName("LastUpdatedDate");
        lkAttr.setLength(20);
        lkAttr.setPrecision(20);
        lkAttr.setScale(20);
        lkAttr.setPhysicalDataType(Schema.Type.LONG.toString());
        lkAttr.setSourceLogicalDataType("Date");
        lkAttr.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);

        Attribute spamIndicator = new Attribute();
        spamIndicator.setName("SPAM_INDICATOR");
        spamIndicator.setDisplayName("SpamIndicator");
        spamIndicator.setLength(20);
        spamIndicator.setPrecision(-1);
        spamIndicator.setScale(-1);
        spamIndicator.setPhysicalDataType(Schema.Type.BOOLEAN.toString());
        spamIndicator.setSourceLogicalDataType("Boolean");
        spamIndicator.setApprovedUsage(ModelingMetadata.MODEL_AND_ALL_INSIGHTS_APPROVED_USAGE);

        Attribute activeRetirementParticipants = new Attribute();
        activeRetirementParticipants.setName("ActiveRetirementParticipants");
        activeRetirementParticipants.setDisplayName("Active Retirement Plan Participants");
        activeRetirementParticipants.setLength(5);
        activeRetirementParticipants.setPrecision(0);
        activeRetirementParticipants.setScale(0);
        activeRetirementParticipants.setPhysicalDataType(Schema.Type.INT.toString());
        activeRetirementParticipants.setSourceLogicalDataType("Integer");
        activeRetirementParticipants.setApprovedUsage(ModelingMetadata.MODEL_APPROVED_USAGE);
        activeRetirementParticipants.setCategory("Firmographics");
        activeRetirementParticipants.setDataType("Int");
        activeRetirementParticipants.setFundamentalType("numeric");
        activeRetirementParticipants.setStatisticalType("ratio");
        activeRetirementParticipants.setTags(ModelingMetadata.EXTERNAL_TAG);
        activeRetirementParticipants.setDataSource("DerivedColumns");

        table.addAttribute(pkAttr);
        table.addAttribute(lkAttr);
        table.addAttribute(spamIndicator);
        table.addAttribute(activeRetirementParticipants);

        return table;
    }

    protected PrimaryKey createPrimaryKey() {
        PrimaryKey pk = new PrimaryKey();
        pk.setName("PK_ID");
        pk.setDisplayName("Primary Key for ID column");
        pk.addAttribute("ID");

        return pk;
    }

    protected Extract createExtract(String name) {
        Extract e = new Extract();
        e.setName(name);
        e.setPath("/" + name);
        e.setExtractionTimestamp(System.currentTimeMillis());
        return e;
    }

    protected LastModifiedKey createLastModifiedKey() {
        LastModifiedKey lk = new LastModifiedKey();
        lk.setName("LK_LUD");
        lk.setDisplayName("Last Modified Key for LastUpdatedDate column");
        lk.addAttribute("LID");

        return lk;
    }

}
