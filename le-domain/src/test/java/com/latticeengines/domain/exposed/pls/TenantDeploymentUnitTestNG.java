package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;

import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class TenantDeploymentUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() {
        TenantDeployment deployment = new TenantDeployment();
        deployment.setTenantId(987L);
        DateTime dt = new DateTime(DateTimeZone.UTC);
        deployment.setCreateTime(new Timestamp(dt.getMillis()));
        deployment.setCreatedBy("unittester@lattice.com");
        deployment.setStep(TenantDeploymentStep.ENTER_CREDENTIALS);
        deployment.setStatus(TenantDeploymentStatus.NEW);
        deployment.setModelId("ms__8195dcf1-0898-4ad3-b94d-0d0f806e979e-PLSModel-Eloqua");
        deployment.setCurrentLaunchId(567L);
        deployment.setMessage("message");

        String serializedStr = deployment.toString();
        TenantDeployment deserializedDeployment = JsonUtils.deserialize(serializedStr, TenantDeployment.class);

        assertEquals(deserializedDeployment.getTenantId(), deployment.getTenantId());
        assertEquals(deserializedDeployment.getCreateTime(), deployment.getCreateTime());
        assertEquals(deserializedDeployment.getCreatedBy(), deployment.getCreatedBy());
        assertEquals(deserializedDeployment.getStep(), deployment.getStep());
        assertEquals(deserializedDeployment.getStatus(), deployment.getStatus());
        assertEquals(deserializedDeployment.getModelId(), deployment.getModelId());
        assertEquals(deserializedDeployment.getCurrentLaunchId(), deployment.getCurrentLaunchId());
        assertEquals(deserializedDeployment.getMessage(), deserializedDeployment.getMessage());
    }
}
