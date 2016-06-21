package com.latticeengines.domain.exposed.metadata;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class ArtifactUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() {
        Artifact artifact = new Artifact();
        artifact.setName("abc");
        artifact.setArtifactType(ArtifactType.PMML);
        
        String serializedStr = JsonUtils.serialize(artifact);
        System.out.println(serializedStr);
        
        Artifact deserializedArtifact = JsonUtils.deserialize(serializedStr, Artifact.class);
        
        assertEquals(deserializedArtifact.getArtifactType(), artifact.getArtifactType());
    }
}
