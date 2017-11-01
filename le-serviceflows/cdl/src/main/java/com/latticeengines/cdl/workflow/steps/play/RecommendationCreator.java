package com.latticeengines.cdl.workflow.steps.play;

import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.dante.DanteLeadDTO;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;

@Component
public class RecommendationCreator {

    private static final double DEFAULT_LIKELIHOOD = 50.0D;

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchProcessor.class);

    @Autowired
    private RecommendationService recommendationService;

    @Autowired
    private DanteLeadProxy danteLeadProxy;

    public void generateRecommendations(PlayLaunchContext playLaunchContext, List<Map<String, Object>> accountList,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList) {
        accountList//
                .stream().parallel() //
                .forEach( //
                        account -> {
                            try {
                                processSingleAccount(playLaunchContext, //
                                        mapForAccountAndContactList, account);
                            } catch (Throwable th) {
                                log.error(th.getMessage(), th);
                                playLaunchContext.getCounter().getAccountErrored().addAndGet(1);
                            }
                        });
    }

    private void processSingleAccount(PlayLaunchContext playLaunchContext,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, Map<String, Object> account) {
        RuleBucketName bucket = getBucketInfo(playLaunchContext, account);

        // Generate recommendation only when bucket is selected for launch
        if (playLaunchContext.getPlayLaunch().getBucketsToLaunch().contains(bucket)) {

            // prepare recommendation
            Recommendation recommendation = //
                    prepareRecommendation(playLaunchContext, account, mapForAccountAndContactList, bucket);

            // insert recommendation in table
            recommendationService.create(recommendation);

            // insert recommendation in dante
            danteLeadProxy.create(
                    new DanteLeadDTO(recommendation, playLaunchContext.getPlay(), //
                            playLaunchContext.getPlayLaunch()), //
                    playLaunchContext.getCustomerSpace().toString());

            // update corresponding counters
            playLaunchContext.getCounter().getContactLaunched().addAndGet(
                    recommendation.getExpandedContacts() != null ? recommendation.getExpandedContacts().size() : 0);
            playLaunchContext.getCounter().getAccountLaunched().addAndGet(1);
        } else {
            playLaunchContext.getCounter().getAccountSuppressed().addAndGet(1);
        }
    }

    private RuleBucketName getBucketInfo(PlayLaunchContext playLaunchContext, Map<String, Object> account) {
        String bucketName = checkAndGet(account, playLaunchContext.getModelId());
        RuleBucketName bucket = RuleBucketName.getRuleBucketName(bucketName);

        if (bucket == null) {
            bucket = RuleBucketName.valueOf(bucketName);
        }
        return bucket;
    }

    private Recommendation prepareRecommendation(PlayLaunchContext playLaunchContext, Map<String, Object> account,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, RuleBucketName bucket) {
        PlayLaunch playLaunch = playLaunchContext.getPlayLaunch();
        long launchTimestampMillis = playLaunchContext.getLaunchTimestampMillis();
        String playName = playLaunchContext.getPlayName();
        String playLaunchId = playLaunchContext.getPlayLaunchId();
        Tenant tenant = playLaunchContext.getTenant();

        Object accountId = checkAndGet(account, InterfaceName.AccountId.name());
        if (accountId == null) {
            throw new RuntimeException("Account Id can not be null");
        }

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLaunch.getPlay().getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLaunch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(accountId.toString());
        recommendation.setLeAccountExternalID(accountId.toString());
        recommendation.setSfdcAccountID(checkAndGet(account, InterfaceName.SalesforceAccountID.name()));
        Double value = 0D;
        recommendation.setMonetaryValue(value);

        // give preference to lattice data cloud field LDC_Name. If not found
        // then try to get company name from customer data itself.
        recommendation.setCompanyName(checkAndGet(account, InterfaceName.LDC_Name.name()));
        if (recommendation.getCompanyName() == null) {
            recommendation.setCompanyName(checkAndGet(account, InterfaceName.CompanyName.name()));
        }

        recommendation.setTenantId(tenant.getPid());
        recommendation.setLikelihood(DEFAULT_LIKELIHOOD);
        recommendation.setSynchronizationDestination(PlaymakerConstants.SFDC);

        recommendation.setPriorityID(bucket);
        recommendation.setPriorityDisplayName(bucket.getName());

        if (mapForAccountAndContactList.containsKey(accountId)) {
            List<Map<String, String>> contactsForRecommendation = PlaymakerUtils
                    .generateContactForRecommendation(mapForAccountAndContactList.get(accountId));
            recommendation.setExpandedContacts(contactsForRecommendation);
        }

        return recommendation;
    }

    private String checkAndGet(Map<String, Object> account, String columnName) {
        return account.get(columnName) != null ? account.get(columnName).toString() : null;
    }

    @VisibleForTesting
    void setRecommendationService(RecommendationService recommendationService) {
        this.recommendationService = recommendationService;
    }

    @VisibleForTesting
    void setDanteLeadProxy(DanteLeadProxy danteLeadProxy) {
        this.danteLeadProxy = danteLeadProxy;
    }
}
