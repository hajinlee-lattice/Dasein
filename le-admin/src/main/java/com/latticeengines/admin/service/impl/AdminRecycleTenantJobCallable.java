package com.latticeengines.admin.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.service.UserService;
public class AdminRecycleTenantJobCallable implements Callable<Boolean> {

    private static final long InAcessPeriod = TimeUnit.DAYS.toMillis(120);
    private static final long emailPeriod = TimeUnit.DAYS.toMillis(14);
    @SuppressWarnings("unused")
    private String jobArguments;

    private EmailService emailService;
    private UserService userService;
    private com.latticeengines.admin.service.TenantService adminTenantService;
    private com.latticeengines.security.exposed.service.TenantService tenantService;
    private static final Logger log = LoggerFactory.getLogger(AdminRecycleTenantJobCallable.class);
    public AdminRecycleTenantJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.emailService = builder.emailService;
        this.userService = builder.userService;
        this.adminTenantService = builder.adminTenantService;
        this.tenantService = builder.tenantService;
    }

    @Override
    public Boolean call() throws Exception {
        List<Tenant> tempTenants = tenantService.getTenantByType(TenantType.POC);
        for (Tenant tenant : tempTenants) {
            long expiredTime = tenant.getExpiredTime();
            long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            // send email two weeks before user can't access tenant
            if (expiredTime - emailPeriod < currentTime && currentTime < expiredTime) {
                int days = (int) Math.ceil((expiredTime - currentTime) / TimeUnit.DAYS.toMillis(1));
                List<User> users = userService.getUsers(tenant.getId());
                users.forEach(user -> {
                    emailService.sendPOCTenantStateNoticeEmail(user, tenant, "Inaccessible", days);
                    log.info(String.format("send POC tenant %s inactive notification to user %s.", tenant.getName(),
                            user.getUsername()));
                });
            } else if (currentTime > expiredTime && TenantStatus.ACTIVE.equals(tenant.getStatus())) {
                tenant.setStatus(TenantStatus.INACTIVE);
                tenantService.updateTenant(tenant);
                log.info(String.format("change POC tenant %s status to inactive", tenant.getName()));
            } else if (expiredTime + InAcessPeriod - emailPeriod < currentTime
                    && currentTime < expiredTime + InAcessPeriod) {
                // send email to user who can visit tenant two weeks before
                // delete tenant
                int days = (int) Math.ceil((expiredTime + InAcessPeriod - currentTime) / TimeUnit.DAYS.toMillis(1));
                List<User> users = userService.getUsers(tenant.getId());
                users.forEach(user -> {
                    emailService.sendPOCTenantStateNoticeEmail(user, tenant, "Deleted", days);
                    log.info(String.format("send POC tenant %s inaccessible notification to user %s.", tenant.getName(),
                            user.getUsername()));
                });

            } else if (currentTime > expiredTime + InAcessPeriod) {
                CustomerSpace space = CustomerSpace.parse(tenant.getId());
                adminTenantService.deleteTenant("_defaultUser", space.getContractId(), space.getTenantId(), true);
                log.info(String.format("POC tenant %s has been deleted", tenant.getName()));
            }
        }
        return null;
    }

    public static class Builder {
        private String jobArguments;
        private EmailService emailService;
        private UserService userService;
        private com.latticeengines.admin.service.TenantService adminTenantService;
        private com.latticeengines.security.exposed.service.TenantService tenantService;

        public Builder() {

        }
        public Builder jobArguments(String jobArguments) {
            this.jobArguments = jobArguments;
            return this;
        }

        public Builder emailService(EmailService emailService) {
            this.emailService = emailService;
            return this;
        }

        public Builder userService(UserService userService) {
            this.userService = userService;
            return this;
        }

        public Builder adminTenantService(com.latticeengines.admin.service.TenantService adminTenantService) {
            this.adminTenantService = adminTenantService;
            return this;
        }

        public Builder securityTenantService(
                com.latticeengines.security.exposed.service.TenantService tenantService) {
            this.tenantService = tenantService;
            return this;
        }
    }
}
