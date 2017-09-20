package com.latticeengines.leadprioritization.workflow.steps.play;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
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

    public void generateRecommendations(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            String modelId, AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, List<Map<String, Object>> accountList,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, long launchTimestampMillis) {
        accountList//
                .stream().parallel() //
                .forEach( //
                        account -> {
                            try {
                                processSingleAccount(tenant, playLaunch, config, //
                                        modelId, accountLaunched, contactLaunched, accountErrored, accountSuppressed, //
                                        mapForAccountAndContactList, account, launchTimestampMillis);
                            } catch (Throwable th) {
                                log.error(th.getMessage(), th);
                                accountErrored.addAndGet(1);
                                throw th;
                            }
                        });
    }

    private void processSingleAccount(Tenant tenant, PlayLaunch playLaunch, PlayLaunchInitStepConfiguration config,
            String modelId, AtomicLong accountLaunched, AtomicLong contactLaunched, AtomicLong accountErrored,
            AtomicLong accountSuppressed, Map<Object, List<Map<String, String>>> mapForAccountAndContactList,
            Map<String, Object> account, long launchTimestampMillis) {
        Recommendation recommendation = //
                prepareRecommendation(tenant, playLaunch, config, account, mapForAccountAndContactList, modelId,
                        launchTimestampMillis);
        if (playLaunch.getBucketsToLaunch().contains(recommendation.getPriorityID())) {
            recommendationService.create(recommendation);
            danteLeadProxy.create(recommendation, config.getCustomerSpace().toString());
            contactLaunched.addAndGet(
                    recommendation.getExpandedContacts() != null ? recommendation.getExpandedContacts().size() : 0);
            accountLaunched.addAndGet(1);
        } else {
            accountSuppressed.addAndGet(1);
        }
    }

    private Recommendation prepareRecommendation(Tenant tenant, PlayLaunch playLaunch,
            PlayLaunchInitStepConfiguration config, Map<String, Object> account,
            Map<Object, List<Map<String, String>>> mapForAccountAndContactList, String modelId,
            long launchTimestampMillis) {
        String playName = config.getPlayName();
        String playLaunchId = config.getPlayLaunchId();
        Object accountIdObj = checkAndGet(account, InterfaceName.AccountId.name());
        Object accountIdLongObj = checkAndGet(account, InterfaceName.LEAccountIDLong.name());
        String accountId = accountIdObj == null ? null : accountIdObj.toString();
        String accountIdLong = accountIdLongObj == null ? accountId : accountIdLongObj.toString();

        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(playLaunch.getPlay().getDescription());
        recommendation.setLaunchId(playLaunchId);
        recommendation.setPlayId(playName);

        Date launchTime = playLaunch.getCreated();
        if (launchTime == null) {
            launchTime = new Date(launchTimestampMillis);
        }
        recommendation.setLaunchDate(launchTime);

        recommendation.setAccountId(accountIdLong);
        recommendation.setLeAccountExternalID(accountIdLong);
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

        String bucketName = checkAndGet(account, modelId);
        RuleBucketName bucket = null;
        bucket = RuleBucketName.getRuleBucketName(bucketName);

        if (bucket == null) {
            bucket = RuleBucketName.valueOf(bucketName);
        }

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
