package com.latticeengines.admin.service.impl;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserTenantRightEntityMgr;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.auth.GlobalAuthUserTenantRight;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.proxy.exposed.admin.AdminProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

public class AdminRecycleTenantJobCallable implements Callable<Boolean> {

    private static final long INACTIVE_PERIOD = TimeUnit.DAYS.toMillis(30);
    private static final long EMAIL_PERIOD = TimeUnit.DAYS.toMillis(14);
    private static final List<String> userLevels =
            AccessLevel.getInternalAccessLevel().stream()
            .map(accessLevel -> accessLevel.toString())
            .collect(Collectors.toList());
    @SuppressWarnings("unused")
    private String jobArguments;

    private EmailService emailService;
    private UserService userService;
    private AdminProxy adminProxy;
    private com.latticeengines.security.exposed.service.TenantService tenantService;
    private GlobalAuthUserTenantRightEntityMgr userTenantRightEntityMgr;
    private BatonService batonService;

    private static final Logger log = LoggerFactory.getLogger(AdminRecycleTenantJobCallable.class);

    public AdminRecycleTenantJobCallable(Builder builder) {
        this.jobArguments = builder.jobArguments;
        this.emailService = builder.emailService;
        this.userService = builder.userService;
        this.adminProxy = builder.adminProxy;
        this.tenantService = builder.tenantService;
        this.userTenantRightEntityMgr = builder.userTenantRightEntityMgr;
        this.batonService = builder.batonService;
    }

    @Override
    public Boolean call() throws Exception {
        scanInvalidTenants();
        List<Tenant> tempTenants = tenantService.getTenantByTypes(Arrays.asList(TenantType.POC, TenantType.STAGING));
        if (CollectionUtils.isNotEmpty(tempTenants)) {
            log.info("Tenants size is " + tempTenants.size());
            for (Tenant tenant : tempTenants) {
                log.info("begin dealing with tenant " + tenant.getName());
                if (tenant.getExpiredTime() == null) {
                    continue;
                }
                CustomerSpace space = CustomerSpace.parse(tenant.getId());
                if (batonService.hasProduct(space, LatticeProduct.DCP)) {
                    log.info("skip recycling the dnb connect tenant {}", space);
                    continue;
                }
                long expiredTime = tenant.getExpiredTime();
                long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();

                // send email two weeks before user can't access tenant
                if (expiredTime - EMAIL_PERIOD < currentTime && currentTime < expiredTime) {
                    int days = (int) Math.ceil((expiredTime - currentTime) / TimeUnit.DAYS.toMillis(1));
                    List<User> users = userService.getUsers(tenant.getId());
                    users.forEach(user -> {
                        if (userLevels.contains(user.getAccessLevel())) {
                            emailService.sendTenantStateNoticeEmail(user, tenant, "Inaccessible", days);
                            log.info(String.format("send tenant %s inactive notification to user %s.",
                                    tenant.getName(), user.getUsername()));
                        }
                    });
                } else if (currentTime > expiredTime && TenantStatus.ACTIVE.equals(tenant.getStatus())) {
                    tenant.setStatus(TenantStatus.INACTIVE);
                    tenantService.updateTenant(tenant);
                    log.info(String.format("change tenant %s status to inactive", tenant.getName()));
                } else if (expiredTime + INACTIVE_PERIOD - EMAIL_PERIOD < currentTime
                        && currentTime < expiredTime + INACTIVE_PERIOD) {
                    // send email to user who can visit tenant two weeks before
                    // delete tenant
                    int days =
                            (int) Math.ceil((expiredTime + INACTIVE_PERIOD - currentTime) / TimeUnit.DAYS.toMillis(1));
                    List<User> users = userService.getUsers(tenant.getId());
                    users.forEach(user -> {
                        if (userLevels.contains(user.getAccessLevel())) {
                            emailService.sendTenantStateNoticeEmail(user, tenant, "Deleted", days);
                            log.info(String.format("send tenant %s inaccessible notification to user %s.",
                                    tenant.getName(), user.getUsername()));
                        }
                    });

                } else if (currentTime > expiredTime + INACTIVE_PERIOD) {
                    adminProxy.deleteTenant(space.getContractId(), space.getTenantId());
                    log.info(String.format("tenant %s has been deleted", tenant.getName()));
                }

            }
        }

        List<GlobalAuthUserTenantRight> tenantRights = userTenantRightEntityMgr.findByNonNullExpirationDate();
        if (CollectionUtils.isNotEmpty(tenantRights)) {
            log.info("expired tenant right size is " + tenantRights.size());
            for (GlobalAuthUserTenantRight tenantRight : tenantRights) {
                if (tenantRight.getGlobalAuthTenant() == null && tenantRight.getGlobalAuthUser() == null) {
                    log.info("orphan record in tenant right " + tenantRight.getPid());
                    continue;
                }
                long currentTime = LocalDateTime.now().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
                long expiredTime = tenantRight.getExpirationDate();
                if (expiredTime - EMAIL_PERIOD < currentTime && currentTime < expiredTime) {
                    int days = (int) Math.ceil((expiredTime - currentTime) / TimeUnit.DAYS.toMillis(1));
                    sendEmail(tenantRight, days);

                } else if (currentTime >= expiredTime) {
                    String tenantId = tenantRight.getGlobalAuthTenant().getId();
                    String userName = tenantRight.getGlobalAuthUser().getEmail();
                    log.info(String.format("Quartz job deleted %s from user %s", tenantId, userName));
                    userService.deleteUser(tenantId, userName, false);
                    sendEmail(tenantRight, 0);
                }
            }
        }
        return true;
    }

    private void scanInvalidTenants() {
        log.info("begin scanning invalid tenants.");
        List<String> tenantIds = tenantService.getAllTenantIds();
        List<String> tenantIdsFromZK = adminProxy.getAllTenantIds();
        if (CollectionUtils.isNotEmpty(tenantIds) && CollectionUtils.isNotEmpty(tenantIdsFromZK)) {
            Set<String> idsInDBNotZK =
                    tenantIds.stream().filter(id -> !tenantIdsFromZK.contains(id)).collect(Collectors.toSet());
            Set<String> idsInZKNotDB =
                    tenantIdsFromZK.stream().filter(id -> !tenantIds.contains(id)).collect(Collectors.toSet());
            log.info("tenants in db not in zk are {}", idsInDBNotZK);
            log.info("tenants in zk not in db are {}", idsInZKNotDB);
        }
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
        private AdminProxy adminProxy;
        private com.latticeengines.security.exposed.service.TenantService tenantService;
        private GlobalAuthUserTenantRightEntityMgr userTenantRightEntityMgr;
        private BatonService batonService;

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

        public Builder adminProxy(AdminProxy adminProxy) {
            this.adminProxy = adminProxy;
            return this;
        }

        public Builder securityTenantService(TenantService tenantService) {
            this.tenantService = tenantService;
            return this;
        }

        public Builder userTenantRightEntityMgr(GlobalAuthUserTenantRightEntityMgr userTenantRightEntityMgr) {
            this.userTenantRightEntityMgr = userTenantRightEntityMgr;
            return this;
        }

        public Builder batonService(BatonService batonService) {
            this.batonService = batonService;
            return this;
        }

    }
}
