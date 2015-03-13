package com.latticeengines.domain.exposed.pls;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class ModelSummaryUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws Exception {
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
                "com/latticeengines/domain/exposed/pls/modelsummary-eloqua.json");
        ModelSummary summary = ModelSummary.generateFromJSON(modelSummaryFileAsStream);

        Date date1 = new Date(summary.getConstructionTime() * 1000);
        String serializedStr = JsonUtils.serialize(summary);
        assertNotNull(serializedStr);
        summary.getName().equals("PLSModel-Eloqua");
        ModelSummary deserializedSummary = JsonUtils.deserialize(serializedStr, ModelSummary.class);
        assertEquals(deserializedSummary.getTenant().getId(), "FAKE_TENANT");
        Date date2 = new Date(deserializedSummary.getConstructionTime() * 1000);
        assertEquals(date2.compareTo(date1), 0);

        Tenant tenant = new Tenant();
        tenant.setPid(-1L);
        tenant.setRegisteredTime(System.currentTimeMillis());
        tenant.setId("NEW_TENANT");
        tenant.setName("New Tenant");
        summary.setTenant(tenant);

        modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(
            "com/latticeengines/domain/exposed/pls/modelsummary-eloqua.json");
        summary = ModelSummary.generateFromJSON(modelSummaryFileAsStream, tenant);
        serializedStr = JsonUtils.serialize(summary);
        deserializedSummary = JsonUtils.deserialize(serializedStr, ModelSummary.class);
        assertEquals(deserializedSummary.getTenant().getId(), "NEW_TENANT");
    }
}
