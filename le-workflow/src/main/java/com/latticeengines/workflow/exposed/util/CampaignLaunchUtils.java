package com.latticeengines.workflow.exposed.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("campaignLaunchUtils")
public final class CampaignLaunchUtils {

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

    public void checkCampaignLaunchLimitation(CustomerSpace customerSpace, CDLExternalSystemName cdlExternalSystemName,
                                              long addedAccountsCount, long fullContactsCount) {
        if (shouldPublishRecommendationsForS3Launch(customerSpace, cdlExternalSystemName)) {
            if (addedAccountsCount > accountLimit) {
                throw new LedpException(LedpCode.LEDP_18240, new String[]{String.valueOf(BusinessEntity.Account), String.valueOf(accountLimit)});
            }
            if (fullContactsCount > contactLimit) {
                throw new LedpException(LedpCode.LEDP_18240, new String[]{String.valueOf(BusinessEntity.Contact), String.valueOf(contactLimit)});
            }
        }
    }
}
