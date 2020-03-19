package com.latticeengines.cdl.workflow.steps.campaign.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

@Component("campaignLaunchUtils")
public final class CampaignLaunchUtils {

    private static final Logger log = LoggerFactory.getLogger(CampaignLaunchUtils.class);

    @Value("${cdl.campaign.account.limit}")
    private long accountLimit;

    @Value("${cdl.campaign.contact.limit}")
    private long contactLimit;

    public boolean shouldPublishRecommendationsForS3Launch(CustomerSpace customerSpace, CDLExternalSystemName cdlExternalSystemName) {
        if (cdlExternalSystemName != null) {
            switch (cdlExternalSystemName) {
                case Eloqua:
                case Salesforce:
                    return true;
                case AWS_S3:
                    return WorkflowJobUtils.getPublishRecommendationsForS3Launch(customerSpace);
                case Marketo:
                case LinkedIn:
                case GoogleAds:
                case Facebook:
                case Outreach:
                case Others:
                default:
                    return false;
            }
        } else {
            return false;
        }
    }

    public void checkCampaignLaunchAccountLimitation(long accountsCount) {
        log.info("Total account count is {}.", accountsCount);
        if (accountsCount > accountLimit) {
            throw new LedpException(LedpCode.LEDP_18240, new String[]{String.valueOf(BusinessEntity.Account), String.valueOf(accountLimit)});
        }
    }

    public void checkCampaignLaunchContactLimitation(long contactsCount) {
        log.info("Total contact count is {}.", contactsCount);
        if (contactsCount > contactLimit) {
            throw new LedpException(LedpCode.LEDP_18240, new String[]{String.valueOf(BusinessEntity.Contact), String.valueOf(contactLimit)});
        }
    }
}
