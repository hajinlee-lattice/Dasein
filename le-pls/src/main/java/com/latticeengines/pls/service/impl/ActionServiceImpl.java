package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionStatus;
import com.latticeengines.domain.exposed.pls.CancelActionEmailInfo;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.monitor.exposed.service.EmailService;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserFilter;
import com.latticeengines.security.exposed.service.UserService;

@Component("actionService")
public class ActionServiceImpl implements ActionService {

    private static final Logger log = LoggerFactory.getLogger(ActionServiceImpl.class);

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private EmailService emailService;

    @Inject
    private UserService userService;

    @Inject
    private WorkflowJobService workflowJobService;

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
        Action action = actionProxy.cancelAction(tenantId, actionPid);
        sendEmail(action);
        return action;
    }

    private void sendEmail(Action action) {
        if (action.getActionStatus() == ActionStatus.CANCELED) {
            String emailAddress = MultiTenantContext.getEmailAddress();
            log.info("emailAddress is :" + emailAddress);
            Tenant tenant = MultiTenantContext.getTenant();
            log.info("tenant is :" + tenant.getName());
            User actionUser = userService.findByEmail(emailAddress);//the user who canceled this action
            log.info("actionUser is :" + actionUser.getUsername());
            CancelActionEmailInfo cancelActionEmailInfo = new CancelActionEmailInfo();
            cancelActionEmailInfo.setActionUserName(actionUser.getUsername());
            cancelActionEmailInfo.setTenantName(tenant.getName());
            cancelActionEmailInfo.setActionName(getActionName(action));
            log.info("paramArr is :" + cancelActionEmailInfo.toString());
            try {
                if (!emailAddress.equals(SYSTEMEMAIL_ADDRESS)) {
                    emailService.sendPlsActionCancelSuccessEmail(actionUser, appPublicUrl, cancelActionEmailInfo);
                    if (!emailAddress.equals(action.getActionInitiator())) {
                        User user_cancel = userService.findByEmail(action.getActionInitiator());
                        emailService.sendPlsActionCancelSuccessEmail(user_cancel, appPublicUrl, cancelActionEmailInfo);
                    }
                } else {
                    UserFilter filter = user -> {
                        if (StringUtils.isEmpty(user.getAccessLevel())) {
                            return false;
                        }
                        AccessLevel level = AccessLevel.valueOf(user.getAccessLevel());
                        return level.equals(AccessLevel.EXTERNAL_ADMIN);
                    };
                    List<User> users = userService.getUsers(tenant.getId(), filter);
                    if (CollectionUtils.isNotEmpty(users)) {
                        for (User user : users) {
                            emailService.sendPlsActionCancelSuccessEmail(user, appPublicUrl, cancelActionEmailInfo);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("email send failed!");
                e.printStackTrace();
            }
        }
    }

    private String getActionName(Action action) {
        List<Long> pids = new ArrayList<>();
        pids.add(action.getPid());
        List<Job> jobs = workflowJobService.findJobsBasedOnActionIdsAndType(pids, action.getType());
        if (jobs.size() > 0) {
            Job job = jobs.get(0);
            Map<String, String> inputMap = job.getInputs();
            return "import:" + inputMap.get("SOURCE_DISPLAY_NAME");
        }
        return "";
    }

}
