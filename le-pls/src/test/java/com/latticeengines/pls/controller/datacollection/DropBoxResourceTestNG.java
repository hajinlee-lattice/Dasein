package com.latticeengines.pls.controller.datacollection;

import java.util.Arrays;
import java.util.List;

import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DropBoxAccessMode;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

public class DropBoxResourceTestNG extends PlsFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DropBoxResourceTestNG.class);

    @Mock
    private DropBoxProxy dropBoxProxy;

    @Mock
    private UserService userService;

    @Mock
    private EmailService emailService;

    private List<User> users;

    private Tenant tenant;

    private GrantDropBoxAccessResponse response;

    @InjectMocks
    private DropBoxResource dropBoxResource;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        MockitoAnnotations.initMocks(this);
        tenant = new Tenant("tenant1");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);

        User exUser1 = new User();
        exUser1.setAccessLevel(AccessLevel.EXTERNAL_ADMIN.name());
        User exUser2 = new User();
        exUser2.setAccessLevel(AccessLevel.EXTERNAL_ADMIN.name());
        User exUser3 = new User();
        exUser3.setAccessLevel(AccessLevel.EXTERNAL_USER.name());
        users = Arrays.asList(exUser1, exUser2, exUser3);
        Mockito.doReturn(users).when(userService).getUsers(Mockito.any(String.class));

        response = new GrantDropBoxAccessResponse();
        response.setAccessKey("accessKey");
        response.setSecretKey("secretKey");
        response.setRegion("us-east1");
        Mockito.doNothing().when(emailService).sendS3CredentialEmail(Mockito.any(User.class), Mockito.any(Tenant.class),
                Mockito.any(GrantDropBoxAccessResponse.class), Mockito.any(String.class));
        Mockito.doReturn(response).when(dropBoxProxy).grantAccess(Mockito.any(String.class),
                Mockito.any(GrantDropBoxAccessRequest.class));
    }

    @Test(groups = "functional")
    public void testGenerateUIActionBasedOnDropBox() {
        DropBoxSummary dropBoxSummary = new DropBoxSummary();
        dropBoxSummary.setAccessMode(DropBoxAccessMode.LatticeUser);
        UIAction uiAction = dropBoxResource.generateUIActionBasedOnDropBox(dropBoxSummary);
        Assert.assertNotNull(uiAction);
        Assert.assertEquals(uiAction.getTitle(), DropBoxResource.GENERATE_DROPBOX_SUCCESS_TITLE);
        Assert.assertEquals(uiAction.getView(), View.Modal);
        Assert.assertEquals(uiAction.getStatus(), Status.Info);
        Assert.assertNotNull(uiAction.getMessage());
        log.info(uiAction.getMessage());

        dropBoxSummary.setAccessKeyId("key");
        uiAction = dropBoxResource.generateUIActionBasedOnDropBox(dropBoxSummary);
        Assert.assertNotNull(uiAction);
        Assert.assertEquals(uiAction.getTitle(), DropBoxResource.GENERATE_DROPBOX_WARNING_TITLE);
        Assert.assertEquals(uiAction.getView(), View.Modal);
        Assert.assertEquals(uiAction.getStatus(), Status.Warning);
        Assert.assertNotNull(uiAction.getMessage());
        log.info(uiAction.getMessage());
    }

}
