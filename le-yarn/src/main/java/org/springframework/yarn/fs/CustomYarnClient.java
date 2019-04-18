package org.springframework.yarn.fs;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.client.ClientRmOperations;
import org.springframework.yarn.client.ClientRmTemplate;
import org.springframework.yarn.client.CommandYarnClient;

public class CustomYarnClient extends CommandYarnClient {

    private static final Logger log = LoggerFactory.getLogger(CommandYarnClient.class);

    private ClientRmTemplate clientRmTemplate;

    public CustomYarnClient(ClientRmOperations clientRmOperations) {
        super(clientRmOperations);
        this.clientRmTemplate = (ClientRmTemplate) clientRmOperations;
    }

    @Override
    public ApplicationId submitApplication() {
        try {
            clientRmTemplate.afterPropertiesSet();
        } catch (Exception e) {
            log.error("clientRmTemplate refresh properties failed.");
        }
        return submitApplication(true);
    }

    @Override
    public ApplicationId submitApplication(boolean distribute) {
        try {
            try {
                clientRmTemplate.afterPropertiesSet();
            } catch (Exception e) {
                log.error("clientRmTemplate refresh properties failed.");
            }
            ApplicationId applicationId = super.submitApplication(distribute);
            return applicationId;
        } catch (Exception e) {
            if (getConfiguration().getBoolean(YarnConfiguration.RM_HA_ENABLED, false))
            {
                log.info("Retry submit application.");
                performFailover();
                return super.submitApplication(distribute);
            }
            throw e;
        }

    }

    @Override
    public List<ApplicationReport> listApplications() {
        try {
            try {
                clientRmTemplate.afterPropertiesSet();
            } catch (Exception e) {
                log.error("clientRmTemplate refresh properties failed.");
            }
            return super.listApplications();
        } catch (Exception e) {
            if (getConfiguration().getBoolean(YarnConfiguration.RM_HA_ENABLED, false))
            {
                log.info("Retry list applications.");
                performFailover();
                return super.listApplications();
            }
            throw e;
        }
    }

    @Override
    public void killApplication(ApplicationId applicationId) {
        try {
            try {
                clientRmTemplate.afterPropertiesSet();
            } catch (Exception e) {
                log.error("clientRmTemplate refresh properties failed.");
            }
            super.killApplication(applicationId);
        } catch (Exception e) {
            if (getConfiguration().getBoolean(YarnConfiguration.RM_HA_ENABLED, false))
            {
                log.info("Retry kill application.");
                performFailover();
                super.killApplication(applicationId);
            }
            throw e;
        }
    }

    @Override
    public ApplicationReport getApplicationReport(ApplicationId applicationId) {
        try {
            try {
                clientRmTemplate.afterPropertiesSet();
            } catch (Exception e) {
                log.error("clientRmTemplate refresh properties failed.");
            }
            return super.getApplicationReport(applicationId);
        } catch (Exception e) {
            if (getConfiguration().getBoolean(YarnConfiguration.RM_HA_ENABLED, false))
            {
                log.info("Retry get application report.");
                performFailover();
                return super.getApplicationReport(applicationId);
            }
            throw e;
        }
    }

    private void performFailover() {
        Configuration conf = getConfiguration();
        log.info(String.format("RM address before fail over: %s", conf.get(YarnConfiguration.RM_ADDRESS)));
        Collection<String> rmIds = HAUtil.getRMHAIds(conf);
        String[] rmServiceIds = rmIds.toArray(new String[rmIds.size()]);
        int currentIndex = 0;
        String currentHAId = conf.get(YarnConfiguration.RM_HA_ID);
        for (int i = 0; i < rmServiceIds.length; i++) {
            if (currentHAId.equals(rmServiceIds[i])) {
                currentIndex = i;
                break;
            }
        }
        currentIndex = (currentIndex + 1) % rmServiceIds.length;
        conf.set(YarnConfiguration.RM_HA_ID, rmServiceIds[currentIndex]);
        String address = conf.get(YarnConfiguration.RM_ADDRESS + "." + rmServiceIds[currentIndex]);
        String webappAddress = conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS + "."
                + rmServiceIds[currentIndex]);
        String schedulerAddress = conf.get(YarnConfiguration.RM_SCHEDULER_ADDRESS + "."
                + rmServiceIds[currentIndex]);
        conf.set(YarnConfiguration.RM_ADDRESS, address);
        conf.set(YarnConfiguration.RM_WEBAPP_ADDRESS, webappAddress);
        conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, schedulerAddress);
        setConfiguration(conf);
        log.info(String.format("RM address after fail over: %s", conf.get(YarnConfiguration.RM_ADDRESS)));
        try {
            clientRmTemplate.afterPropertiesSet();
        } catch (Exception e) {
            log.error("clientRmTemplate refresh properties failed.");
        }
    }
}
