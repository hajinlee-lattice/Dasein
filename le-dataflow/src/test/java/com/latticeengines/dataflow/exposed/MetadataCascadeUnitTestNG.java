package com.latticeengines.dataflow.exposed;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.dataflow.exposed.builder.MetadataCascade;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import edu.emory.mathcs.backport.java.util.Collections;

public class MetadataCascadeUnitTestNG {

    @Test(groups = "unit")
    public void testCascadeApprovedUsage() {
        List<FieldMetadata> metadata = new ArrayList<>();
        FieldMetadata field = new FieldMetadata("Foo", Integer.class);
        field.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.MODEL).toString());
        metadata.add(field);

        FieldMetadata parent = new FieldMetadata("Parent", Integer.class);
        parent.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.NONE).toString());
        field.addAncestor(parent);

        FieldMetadata grandparent = new FieldMetadata("Grandparent", Integer.class);
        grandparent.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.NONE).toString());
        parent.addAncestor(grandparent);

        MetadataCascade cascade = new MetadataCascade(metadata);
        cascade.cascade();

        assertEquals(field.getPropertyValue("ApprovedUsage"), Collections.singletonList(ApprovedUsage.NONE).toString());
    }

    @Test(groups = "unit")
    public void testDontCascadeApprovedUsage() {
        List<FieldMetadata> metadata = new ArrayList<>();
        FieldMetadata field = new FieldMetadata("Foo", Integer.class);
        field.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.MODEL).toString());
        metadata.add(field);

        FieldMetadata parent = new FieldMetadata("Parent", Integer.class);
        parent.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.MODEL).toString());
        field.addAncestor(parent);

        FieldMetadata grandparent = new FieldMetadata("Grandparent", Integer.class);
        grandparent.setPropertyValue("ApprovedUsage", Collections.singletonList(ApprovedUsage.MODEL).toString());
        parent.addAncestor(grandparent);

        MetadataCascade cascade = new MetadataCascade(metadata);
        cascade.cascade();

        assertEquals(field.getPropertyValue("ApprovedUsage"), Collections.singletonList(ApprovedUsage.MODEL).toString());
    }

}
