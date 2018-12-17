package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;

@Component("actionService")
public class ActionServiceImpl implements ActionService {

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private EmailService emailService;

    @Inject
    private UserService userService;

    @Value("${security.app.public.url:http://localhost:8081}")
    private String appPublicUrl;

    private static final String SYSTEMEMAIL_ADDRESS = "system@lattice-engines.com";

    @Override
    public Action create(Action action) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return actionProxy.createAction(tenantId, action);
    }

    @Override
    public Action cancel(Long actionPid) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return actionProxy.cancelAction(tenantId, actionPid);
    }

    @Override
    public void sendEmail(Action action) {
        if (action.getCanceled()) {
            String emailAddress = MultiTenantContext.getEmailAddress();
            if (!emailAddress.equals(SYSTEMEMAIL_ADDRESS)) {
                User user = userService.findByEmail(emailAddress);
                emailService.sendPlsActionCancelSuccessEmail(user, appPublicUrl);
                if (!emailAddress.equals(action.getActionInitiator())) {
                    User user_cancel = userService.findByEmail(action.getActionInitiator());
                    emailService.sendPlsActionCancelSuccessEmail(user_cancel, appPublicUrl);
                }
            } else {
                Tenant tenant = action.getTenant();
                UserFilter filter = user -> {
                    if (StringUtils.isEmpty(user.getAccessLevel())) {
                        return false;
                    }
                    AccessLevel level = AccessLevel.valueOf(user.getAccessLevel());
                    return level.equals(AccessLevel.EXTERNAL_ADMIN);
                };
                List<User> users = userService.getUsers(tenant.getId(), filter);
                for (User user : users) {
                    emailService.sendPlsActionCancelSuccessEmail(user, appPublicUrl);
                }
            }
        }
    }

}
