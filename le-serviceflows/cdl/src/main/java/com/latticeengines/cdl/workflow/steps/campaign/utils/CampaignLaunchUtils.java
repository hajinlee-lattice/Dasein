package com.latticeengines.cdl.workflow.steps.campaign.utils;

import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.ChannelConfigUtil;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.util.WorkflowJobUtils;

@Component("campaignLaunchUtils")
public final class CampaignLaunchUtils {

    @Value("${cdl.campaign.account.limit}")
    private long accountLimit;

    @Value("${cdl.campaign.contact.limit}")
    private long contactLimit;

    @Inject
    private PlayProxy playProxy;

    public interface QueryCallback<T1, T2, R> {
        R apply(T1 t1, T2 t2);
    }

    public FrontEndQuery buildCampaignFrontEndQuery(CustomerSpace customerSpace, ChannelConfig channelConfig, Play play,
                                                    boolean contactsDataExists, boolean launchUnScored,
                                                    Set<RatingBucketName> launchBuckets, CDLExternalSystemName externalSystemName, Long maxAccountsToLaunch, String lookupId) {
        return new CampaignFrontEndQueryBuilder.Builder() //
                .mainEntity(channelConfig.getAudienceType().asBusinessEntity()) //
                .customerSpace(customerSpace) //
                .baseAccountRestriction(play.getTargetSegment().getAccountRestriction()) //
                .baseContactRestriction(contactsDataExists ? play.getTargetSegment().getContactRestriction() : null)
                .isSuppressAccountsWithoutLookupId(channelConfig.isSuppressAccountsWithoutLookupId()) //
                .isSuppressAccountsWithoutContacts(
                        contactsDataExists && channelConfig.isSuppressAccountsWithoutContacts())
                .isSuppressContactsWithoutEmails(contactsDataExists && channelConfig.isSuppressContactsWithoutEmails())
                .isSuppressAccountsWithoutWebsiteOrCompanyName(ChannelConfigUtil.shouldApplyAccountNameOrWebsiteFilter(
                        externalSystemName, channelConfig))
                .bucketsToLaunch(launchBuckets) //
                .limit(maxAccountsToLaunch) //
                .lookupId(lookupId) //
                .launchUnScored(launchUnScored) //
                .destinationSystemName(externalSystemName) //
                .ratingId(play.getRatingEngine() != null ? play.getRatingEngine().getId() : null) //
                .getCampaignFrontEndQueryBuilder() //
                .build();
    }

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

    public void checkCampaignLaunchLimitation(FrontEndQuery frontEndQuery, boolean contactsDataExists, Long maxAccountsToLaunch,
                                              QueryCallback<FrontEndQuery, BusinessEntity, Long> queryCallback) {
        long accountCount;
        if (maxAccountsToLaunch != null && maxAccountsToLaunch > 0) {
            accountCount = maxAccountsToLaunch;
        } else {
            accountCount = queryCallback.apply(frontEndQuery, BusinessEntity.Account);
        }
        checkCampaignLaunchAccountLimitation(accountCount);
        if (contactsDataExists) {
            long contactCount = queryCallback.apply(frontEndQuery, BusinessEntity.Contact);
            checkCampaignLaunchContactLimitation(contactCount);
        }
    }

    private void checkCampaignLaunchAccountLimitation(long addedAccountsCount) {
        if (addedAccountsCount > accountLimit) {
            throw new LedpException(LedpCode.LEDP_18240, new String[]{String.valueOf(BusinessEntity.Account), String.valueOf(accountLimit)});
        }
    }

    private void checkCampaignLaunchContactLimitation(long contactCount) {
        if (contactCount > contactLimit) {
            throw new LedpException(LedpCode.LEDP_18240, new String[]{String.valueOf(BusinessEntity.Contact), String.valueOf(contactLimit)});
        }
    }

    public Play getPlay(CustomerSpace customerSpace, String playId, boolean considerDeleted, boolean shouldLoadCoverage) {
        Play play = playProxy.getPlay(customerSpace.getTenantId(), playId, considerDeleted, shouldLoadCoverage);
        if (play == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[]{"No Campaign found by ID: " + playId});
        }
        return play;
    }

    public PlayLaunchChannel getPlayLaunchChannel(CustomerSpace customerSpace, String playId, String channelId) {
        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace.getTenantId(), playId, channelId);
        if (channel == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[]{"No Channel found by ID: " + channelId});
        }
        return channel;
    }

    public PlayLaunch getPlayLaunch(CustomerSpace customerSpace, String playId, String launchId) {
        PlayLaunch launch = null;
        if (StringUtils.isNotBlank(launchId)) {
            launch = playProxy.getPlayLaunch(customerSpace.getTenantId(), playId, launchId);
        }
        return launch;
    }
}
