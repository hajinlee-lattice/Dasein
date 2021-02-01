package com.latticeengines.admin.qbean;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.impl.AdminRecycleTenantJobCallable;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.admin.AdminProxy;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

@Component("adminRecycleTenantJob")
public class AdminRecycleTenantJobBean implements QuartzJobBean {
    private static final Logger log = LoggerFactory.getLogger(AdminRecycleTenantJobBean.class);

    @Inject
    private EmailService emailService;

    @Inject
    private UserService userService;

    @Inject
    private AdminProxy adminProxy;

    @Inject
    private TenantService tenantService;

    @Inject
    private GlobalAuthUserTenantRightEntityMgr GlobalAuthUserTenantRightEntityMgr;

    @Inject
    private BatonService batonService;

    @Override
    public Callable<Boolean> getCallable(String jobArguments) {
        log.info(String.format("Got callback with job arguments = %s", jobArguments));

        AdminRecycleTenantJobCallable.Builder builder = new AdminRecycleTenantJobCallable.Builder();
        builder.jobArguments(jobArguments).emailService(emailService).userService(userService)
                .adminProxy(adminProxy).securityTenantService(tenantService)
                .userTenantRightEntityMgr(GlobalAuthUserTenantRightEntityMgr)
                .batonService(batonService);
        return new AdminRecycleTenantJobCallable(builder);
    }

}
