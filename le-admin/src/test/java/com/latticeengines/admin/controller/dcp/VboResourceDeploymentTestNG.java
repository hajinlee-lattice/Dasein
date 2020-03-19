package com.latticeengines.admin.controller.dcp;

import java.util.ArrayList;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminDeploymentTestNGBase;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;

public class VboResourceDeploymentTestNG extends AdminDeploymentTestNGBase {

    @Test(groups = "deployment")
    public void testVboRequest() {
        String fullTenantId = "LETest" + System.currentTimeMillis();
        String url = getRestHostPort() + "/admin/dcp/vboadmin";

        VboRequest req = new VboRequest();
        VboRequest.Product pro = new VboRequest.Product();
        VboRequest.User user = new VboRequest.User();
        VboRequest.Name name = new VboRequest.Name();
        name.setFirstName("test");
        name.setLastName("test");
        user.setName(name);
        user.setEmailAddress("test@test.com");

        pro.setUsers(new ArrayList<VboRequest.User>());
        pro.getUsers().add(user);
        req.setProduct(pro);
        VboRequest.Subscriber sub = new VboRequest.Subscriber();
        sub.setName(fullTenantId);
        req.setSubscriber(sub);

        VboResponse result = restTemplate.postForObject(url, req, VboResponse.class);

        Assert.assertNotNull(result);
        Assert.assertEquals(result.getStatus(), "success");

        try {
            deleteTenant(fullTenantId, fullTenantId);
        } catch (Exception ignore) {
        }
    }
}
