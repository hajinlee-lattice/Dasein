package com.latticeengines.cdl.workflow.steps.campaign;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.QueuePlayLaunchesStepConfiguration;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("queuePlayLaunchesStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class QueuePlayLaunchesStep extends BaseWorkflowStep<QueuePlayLaunchesStepConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(QueuePlayLaunchesStep.class);

    @Inject
    private PlayProxy playProxy;

    @Override
    public void execute() {
        log.info("Queueing Scheduled PlayLaunches");
        String customerSpace = configuration.getCustomerSpace().getTenantId();
        PlayLaunch launch = playProxy.queueNewLaunchByPlayAndChannel(configuration.getCustomerSpace().toString(),
                configuration.getPlayId(), configuration.getChannelId());

        if (StringUtils.isNotBlank(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class))) {
            launch.setAddAccountsTable(getObjectFromContext(ADDED_ACCOUNTS_DELTA_TABLE, String.class));
        }

        if (StringUtils.isNotBlank(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class))) {
            launch.setRemoveAccountsTable(getObjectFromContext(REMOVED_ACCOUNTS_DELTA_TABLE, String.class));
        }

        if (StringUtils.isNotBlank(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class))) {
            launch.setAddContactsTable(getObjectFromContext(ADDED_CONTACTS_DELTA_TABLE, String.class));
        }

        if (StringUtils.isNotBlank(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class))) {
            launch.setRemoveContactsTable(getObjectFromContext(REMOVED_CONTACTS_DELTA_TABLE, String.class));
        }

        playProxy.updatePlayLaunch(configuration.getCustomerSpace().getTenantId(), configuration.getPlayId(),
                launch.getLaunchId(), launch);

        playProxy.setNextScheduledTimeForChannel(configuration.getCustomerSpace().toString(), configuration.getPlayId(),
                configuration.getChannelId());
        log.info("Queued Launch: " + launch.getId());
    }

}
