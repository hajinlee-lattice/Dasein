package com.latticeengines.admin.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

public class AdminRecycleTenantJobCallable implements Callable<Boolean> {

    private static final long inAcessPeriod = TimeUnit.DAYS.toMillis(120);
    private static final long emailPeriod = TimeUnit.DAYS.toMillis(14);
    private static final List<String> userLevels = AccessLevel.getInternalAccessLevel().stream()
            .map(accessLevel -> accessLevel.toString())
            .collect(Collectors.toList());
    @SuppressWarnings("unused")
    private String jobArguments;

    private EmailService emailService;
    private UserService userService;
    private com.latticeengines.admin.service.TenantService adminTenantService;
    private com.latticeengines.security.exposed.service.TenantService tenantService;
    private GlobalAuthUserTenantRightEntityMgr userTenantRightEntityMgr;
    private static final Logger log = LoggerFactory.getLogger(AdminRecycleTenantJobCallable.class);

    public AdminRecycleTenantJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.emailService = builder.emailService;
        this.userService = builder.userService;
        this.adminTenantService = builder.adminTenantService;
        this.tenantService = builder.tenantService;
        this.userTenantRightEntityMgr = builder.userTenantRightEntityMgr;
    }

    @Override
    public Boolean call() throws Exception {
        List<Tenant> tempTenants = tenantService.getTenantByType(TenantType.POC);
        if (CollectionUtils.isNotEmpty(tempTenants)) {
            log.info("POC tennats size is " + tempTenants.size());
            for (Tenant tenant : tempTenants) {
                log.info("begin dealing with tenant " + tenant.getName());
                if (tenant.getExpiredTime() == null) {
                    continue;
                }
                long expiredTime = tenant.getExpiredTime();
                long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                // send email two weeks before user can't access tenant
                if (expiredTime - emailPeriod < currentTime && currentTime < expiredTime) {
                    int days = (int) Math.ceil((expiredTime - currentTime) / TimeUnit.DAYS.toMillis(1));
                    List<User> users = userService.getUsers(tenant.getId());
                    users.forEach(user -> {
                        if (userLevels.contains(user.getAccessLevel())) {
                            emailService.sendPOCTenantStateNoticeEmail(user, tenant, "Inaccessible", days);
                            log.info(String.format("send POC tenant %s inactive notification to user %s.",
                                    tenant.getName(), user.getUsername()));
                        }
                    });
                } else if (currentTime > expiredTime && TenantStatus.ACTIVE.equals(tenant.getStatus())) {
                    tenant.setStatus(TenantStatus.INACTIVE);
                    tenantService.updateTenant(tenant);
                    log.info(String.format("change POC tenant %s status to inactive", tenant.getName()));
                } else if (expiredTime + inAcessPeriod - emailPeriod < currentTime
                        && currentTime < expiredTime + inAcessPeriod) {
                    // send email to user who can visit tenant two weeks before
                    // delete tenant
                    int days = (int) Math.ceil((expiredTime + inAcessPeriod - currentTime) / TimeUnit.DAYS.toMillis(1));
                    List<User> users = userService.getUsers(tenant.getId());
                    users.forEach(user -> {
                        if (userLevels.contains(user.getAccessLevel())) {
                            emailService.sendPOCTenantStateNoticeEmail(user, tenant, "Deleted", days);
                            log.info(String.format("send POC tenant %s inaccessible notification to user %s.",
                                    tenant.getName(), user.getUsername()));
                        }
                    });

                } else if (currentTime > expiredTime + inAcessPeriod) {
                    CustomerSpace space = CustomerSpace.parse(tenant.getId());
                    adminTenantService.deleteTenant("_defaultUser", space.getContractId(), space.getTenantId(), true);
                    log.info(String.format("POC tenant %s has been deleted", tenant.getName()));
                }
            }
        }

        List<GlobalAuthUserTenantRight> tenantRights = userTenantRightEntityMgr.findByNonNullExprationDate();
        if (CollectionUtils.isNotEmpty(tenantRights)) {
            log.info("expired tenant right size is " + tenantRights.size());
            for (GlobalAuthUserTenantRight tenantRight : tenantRights) {
                if (tenantRight.getGlobalAuthTenant() == null && tenantRight.getGlobalAuthUser() == null) {
                    log.info("orphan record in tenant right " + tenantRight.getPid());
                    continue;
                }
                long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                long expiredTime = tenantRight.getExpirationDate();
                if (expiredTime - emailPeriod < currentTime && currentTime < expiredTime) {
                    int days = (int) Math.ceil((expiredTime - currentTime) / TimeUnit.DAYS.toMillis(1));
                    sendEmail(tenantRight, days);

                } else if (currentTime >= expiredTime) {
                    String tenantId = tenantRight.getGlobalAuthTenant().getId();
                    String userName = tenantRight.getGlobalAuthUser().getEmail();
                    log.info(String.format(String.format("Quartz job deleted %s from user %s", tenantId, userName)));
                    userService.deleteUser(tenantId, userName);
                    sendEmail(tenantRight, 0);
                }
            }
        }
        return true;
    }

    private void sendEmail(GlobalAuthUserTenantRight tenantRight, int days) {
        Tenant tenant = tenantService.findByTenantId(tenantRight.getGlobalAuthTenant().getId());
        GlobalAuthUser userData = tenantRight.getGlobalAuthUser();
        if (StringUtils.isBlank(userData.getFirstName()) || StringUtils.isBlank(userData.getEmail())) {
            return ;
        }
        User user = new User();
        user.setFirstName(userData.getFirstName());
        user.setEmail(userData.getEmail());
        emailService.sendTenantRightStatusNoticeEmail(user, tenant, days);
    }

    public static class Builder {
        private String jobArguments;
        private EmailService emailService;
        private UserService userService;
        private com.latticeengines.admin.service.TenantService adminTenantService;
        private com.latticeengines.security.exposed.service.TenantService tenantService;
        private GlobalAuthUserTenantRightEntityMgr userTenantRightEntityMgr;

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

        public Builder userTenantRightEntityMgr(GlobalAuthUserTenantRightEntityMgr userTenantRightEntityMgr) {
            this.userTenantRightEntityMgr = userTenantRightEntityMgr;
            return this;
        }
    }
}
