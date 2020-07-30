package com.latticeengines.admin.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.entitymgr.VboRequestLogEntityMgr;
import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.VboRequestLogService;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.dcp.vbo.VboRequest;
import com.latticeengines.domain.exposed.dcp.vbo.VboResponse;
import com.latticeengines.domain.exposed.vbo.VboRequestLog;

public class VboRequestLogServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Inject
    private VboRequestLogService vboRequestLogService;

    @Inject
    private VboRequestLogEntityMgr vboRequestLogEntityMgr;

    private VboRequestLog requestLog = null;

    @Override
    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        // do nothing.
    }

    @Test(groups = "functional")
    public void testCreateAndGet() {
        VboRequest vboRequest = generateVBORequest();
        VboResponse vboResponse = new VboResponse();
        String traceId = NamingUtils.uuid("TranceId");
        vboResponse.setStatus("success");
        vboResponse.setMessage("Test");
        vboResponse.setAckReferenceId(traceId);
        vboRequestLogService.createVboRequestLog(traceId, "TenantId", vboRequest, null);
        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            requestLog = vboRequestLogService.getVboRequestLogByTraceId(traceId);
            Assert.assertNotNull(requestLog);
            Assert.assertNotNull(requestLog.getVboRequest());
            Assert.assertNull(requestLog.getVboResponse());
            return true;
            });
        vboRequestLogService.updateVboResponse(traceId, vboResponse);
        retry.execute(ctx -> {
            requestLog = vboRequestLogService.getVboRequestLogByTraceId(traceId);
            Assert.assertNotNull(requestLog);
            Assert.assertNotNull(requestLog.getVboRequest());
            Assert.assertNotNull(requestLog.getVboResponse());
            return true;
            });

    }

    @Test(groups = "functional")
    public void testCreateWithNullId() {
        VboRequest vboRequest = generateVBORequest();
        VboResponse vboResponse = new VboResponse();
        String tenantId = NamingUtils.uuid("TenantId");
        vboResponse.setStatus("failed");
        vboResponse.setMessage("Test");
        vboRequestLogService.createVboRequestLog(null, tenantId, vboRequest, vboResponse);
        vboRequestLogService.createVboRequestLog(null, tenantId, vboRequest, vboResponse);
        AtomicReference<List<VboRequestLog>> nullLog = new AtomicReference<>();
        RetryTemplate retry = RetryUtils.getRetryTemplate(5,
                Collections.singleton(AssertionError.class), null);
        retry.execute(ctx -> {
            nullLog.set(vboRequestLogService.getVboRequestLogByTenantId(tenantId));
            Assert.assertNotNull(nullLog);
            Assert.assertEquals(nullLog.get().size(), 2);
            return true;
                });

        nullLog.get().forEach(log -> vboRequestLogEntityMgr.delete(log));
    }

    private VboRequest generateVBORequest() {
        VboRequest req = new VboRequest();
        VboRequest.Product pro = new VboRequest.Product();
        VboRequest.User user = new VboRequest.User();
        VboRequest.Name name = new VboRequest.Name();
        name.setFirstName("test2");
        name.setLastName("test2");
        user.setName(name);
        user.setUserId("testdcp2");
        user.setEmailAddress("testdcp@dnb.com");
        user.setTelephoneNumber("1234567");

        pro.setUsers(new ArrayList<>());
        pro.getUsers().add(user);
        req.setProduct(pro);
        VboRequest.Subscriber sub = new VboRequest.Subscriber();
        sub.setLanguage("English");
        sub.setName("tenantId");
        sub.setSubscriberNumber("subNumber");
        req.setSubscriber(sub);
        return req;
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        if (requestLog != null) {
            vboRequestLogEntityMgr.delete(requestLog);
        }
    }

}
