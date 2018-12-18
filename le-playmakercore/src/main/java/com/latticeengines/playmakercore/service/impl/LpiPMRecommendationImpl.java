package com.latticeengines.playmakercore.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("lpiPMRecommendation")
public class LpiPMRecommendationImpl implements LpiPMRecommendation {

    private static final Logger log = LoggerFactory.getLogger(LpiPMRecommendationImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private RecommendationEntityMgr recommendationEntityMgr;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private CleanupExecutor cleanupExecutor;

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum,
            SynchronizationDestinationEnum syncDestination, List<String> playIds, Map<String, String> orgInfo,
            Map<String, String> appId) {
        List<Map<String, Object>> data = recommendationEntityMgr.findRecommendationsAsMap(
                PlaymakerUtils.dateFromEpochSeconds(start), offset, maximum, syncDestination.name(), playIds, orgInfo);
        return postProcess(data, offset);
    }

    @Override
    public List<Map<String, Object>> getRecommendationsByLaunchIds(List<String> launchIds, long start, int offset, int maximum) {
        List<Map<String, Object>> data = recommendationEntityMgr.findRecommendationsAsMapByLaunchIds(launchIds, start, offset,
                maximum);
        return postProcess(data, offset);
    }

    @Override
    public int getRecommendationCountByLaunchIds(List<String> launchIds, long start) {
        return recommendationEntityMgr.findRecommendationCountByLaunchIds(launchIds,start);
    }

    private List<Map<String, Object>> postProcess(List<Map<String, Object>> data, int offset) {

        List<Play> plays = playProxy.getPlays(MultiTenantContext.getCustomerSpace().toString(), null, null);
        Map<String, Triple<Long, String, String>> playNameAndPidMap = new HashMap<>();
        for (Play play : plays) {
            playNameAndPidMap.put(play.getName(),
                    new ImmutableTriple<>(play.getPid(), play.getDisplayName(), play.getDescription()));
        }

        Map<String, Long> playLaunchNameAndPidMap = new HashMap<>();

        if (CollectionUtils.isNotEmpty(data)) {
            int rowNum = offset + 1;

            for (Map<String, Object> accExtRec : data) {

                if (accExtRec.containsKey(PlaymakerConstants.AccountID)
                        && accExtRec.get(PlaymakerConstants.AccountID) != null) {
                    String accountId = (String) accExtRec.get(PlaymakerConstants.AccountID);
                    if (StringUtils.isNotBlank(accountId) && StringUtils.isNumeric(accountId)) {
                        Long longAccId = Long.parseLong(accountId);
                        accExtRec.put(PlaymakerConstants.AccountID, longAccId);
                    } else {
                        // remove AccountID from response as BIS expects it into
                        // numerical format
                        accExtRec.put(PlaymakerConstants.AccountID, null);
                    }
                    accExtRec.put(PlaymakerConstants.AccountID + PlaymakerConstants.V2, accountId);
                }

                if (accExtRec.containsKey(PlaymakerConstants.PlayID)) {
                    String playName = (String) accExtRec.get(PlaymakerConstants.PlayID);

                    if (playNameAndPidMap.containsKey(playName)) {
                        accExtRec.put(PlaymakerConstants.PlayID, playNameAndPidMap.get(playName).getLeft());
                        accExtRec.put(PlaymakerConstants.PlayID + PlaymakerConstants.V2, playName);
                        accExtRec.put(PlaymakerConstants.DisplayName, playNameAndPidMap.get(playName).getMiddle());

                        if (accExtRec.get(PlaymakerConstants.Description) == null) {
                            accExtRec.put(PlaymakerConstants.Description, playNameAndPidMap.get(playName).getRight());
                        }

                        if (accExtRec.containsKey(PlaymakerConstants.LaunchID)) {
                            String launchName = (String) accExtRec.get(PlaymakerConstants.LaunchID);
                            if (!playLaunchNameAndPidMap.containsKey(launchName)) {
                                PlayLaunch launch = playProxy.getPlayLaunch(
                                        MultiTenantContext.getCustomerSpace().toString(), playName, launchName);
                                if (launch != null) {
                                    playLaunchNameAndPidMap.put(launchName, launch.getPid());
                                }
                            }
                            accExtRec.put(PlaymakerConstants.LaunchID, playLaunchNameAndPidMap.get(launchName));
                            accExtRec.put(PlaymakerConstants.LaunchID + PlaymakerConstants.V2, launchName);
                        }
                    } else {
                        log.error("Play info not found for recommendation - play: " + playName
                                + ". Ignoring this error to get rest of the valid recommendations");

                    }
                }

                if (accExtRec.containsKey(PlaymakerConstants.LaunchDate)) {
                    accExtRec.put(PlaymakerConstants.ExpirationDate,
                            (long) accExtRec.get(PlaymakerConstants.LaunchDate) + TimeUnit.DAYS.toSeconds(6 * 31));
                }

                Object bucketEnumObj = accExtRec.get(PlaymakerConstants.PriorityID);
                if (bucketEnumObj != null && StringUtils.isNotBlank(bucketEnumObj.toString())) {
                    String bucketEnumString = bucketEnumObj.toString();
                    RatingBucketName bucket = RatingBucketName.valueOf(bucketEnumString);
                    accExtRec.put(PlaymakerConstants.PriorityID, bucket.ordinal());
                } else {
                    accExtRec.put(PlaymakerConstants.PriorityID, 25);
                }

                accExtRec.put(PlaymakerConstants.SfdcContactID, "");
                List<Map<String, String>> contactList = PlaymakerUtils
                        .getExpandedContacts((String) accExtRec.get(PlaymakerConstants.Contacts));

                accExtRec.put(PlaymakerConstants.Contacts, //
                        contactList.isEmpty() //
                                ? new ArrayList<>() //
                                : contactList);

                accExtRec.put(PlaymakerConstants.RowNum, rowNum++);
            }

        }

        return data;
    }

    @Override
    public int getRecommendationCount(long start, SynchronizationDestinationEnum syncDestination, List<String> playIds,
            Map<String, String> orgInfo, Map<String, String> appId) {
        return recommendationEntityMgr.findRecommendationCount(PlaymakerUtils.dateFromEpochSeconds(start),
                syncDestination.name(), playIds, orgInfo);
    }

    @Override
    public Recommendation getRecommendationById(String recommendationId) {
        return recommendationEntityMgr.findByRecommendationId(recommendationId);
    }

    @Override
    public int cleanupRecommendations(String playId) {
        return cleanupExecutor.cleanupRecommendations(playId);
    }

    @Override
    public int cleanupOldRecommendationsBeforeCutoffDate(Date cutoffDate) {
        return cleanupExecutor.cleanupOldRecommendationsBeforeCutoffDate(cutoffDate);
    }

    @Component
    class CleanupExecutor {

        private Logger cleanupExecutorLog = LoggerFactory.getLogger(CleanupExecutor.class);

        @Value("${playmaker.update.bulk.max:1000}")
        private int maxUpdateRows;

        public int cleanupRecommendations(String playId) {
            String tenantId = MultiTenantContext.getCustomerSpace().toString();
            boolean shouldLoop = true;
            int deletedCount = 0;
            int idx = 0;
            try {
                while (shouldLoop) {
                    int updatedCount = recommendationEntityMgr.deleteInBulkByPlayId(playId, null, true, maxUpdateRows);
                    shouldLoop = updatedCount > 0;
                    deletedCount += updatedCount;
                    if (shouldLoop) {
                        cleanupExecutorLog.info(String.format(
                                "cleanupRecommendations: Tenant = %s, Loop idx = %d, "
                                        + "maxUpdateRows = %d, actualUpdatedCount = %d",
                                tenantId, idx++, maxUpdateRows, updatedCount));
                    }
                }
                if (deletedCount > 0) {
                    cleanupExecutorLog.info(
                            String.format("cleanupRecommendations: Tenant = %s, Completed cleanup recommendations "
                                    + "(count = %d) for playId = %s", tenantId, deletedCount, playId));
                }

                Play play = playProxy.getPlay(tenantId, playId, true, false);
                play.setIsCleanupDone(true);
                playProxy.createOrUpdatePlay(tenantId, play, false);
                cleanupExecutorLog.info(String.format("cleanupRecommendations: Tenant = %s, Marked deleted playId = %s "
                        + "with cleanupDone flag set to true", tenantId, playId));
            } catch (Exception ex) {
                cleanupExecutorLog.error(String.format(
                        "cleanupRecommendations: Tenant = %s, Failed to cleanup recommendations for playId = %s",
                        tenantId, playId), ex);
            }

            return deletedCount;
        }

        public int cleanupOldRecommendationsBeforeCutoffDate(Date cutoffDate) {
            String tenantId = MultiTenantContext.getCustomerSpace().toString();
            boolean shouldLoop = true;
            int deletedCount = 0;
            int idx = 0;
            try {
                long timestamp = System.currentTimeMillis();
                while (shouldLoop) {
                    int updatedCount = recommendationEntityMgr.deleteInBulkByCutoffDate(cutoffDate, false,
                            maxUpdateRows);
                    shouldLoop = updatedCount > 0;
                    deletedCount += updatedCount;
                    if (shouldLoop) {
                        cleanupExecutorLog.info(String.format(
                                "cleanupOldRecommendationsBeforeCutoffDate: Tenant = %s, Loop idx = %d, "
                                        + "maxUpdateRows = %d, actualUpdatedCount = %d",
                                tenantId, idx++, maxUpdateRows, updatedCount));
                    }
                }

                if (deletedCount > 0) {
                    cleanupExecutorLog.info(String.format(
                            "cleanupOldRecommendationsBeforeCutoffDate: Tenant = %s, Completed cleanup "
                                    + "very old recommendations (count = %d) with cutoffDate = %s in %d milliseconds",
                            tenantId, deletedCount, cutoffDate, (System.currentTimeMillis() - timestamp)));
                }
            } catch (Exception ex) {
                cleanupExecutorLog.error(
                        String.format("cleanupOldRecommendationsBeforeCutoffDate: Tenant = %s, Failed to cleanup "
                                + "very old recommendations with cutoffDate = %s", tenantId, cutoffDate),
                        ex);
            }
            return deletedCount;
        }
    }

    @VisibleForTesting
    public void setRecommendationEntityMgr(RecommendationEntityMgr recommendationEntityMgr) {
        this.recommendationEntityMgr = recommendationEntityMgr;
    }

    @VisibleForTesting
    public void setPlayProxy(PlayProxy playProxy) {
        this.playProxy = playProxy;
    }

    @Override
    public List<Map<String, Object>> getAccountIdsFromRecommendationByLaunchId(List<String> launchIds, long start, int offset, int max) {
        return recommendationEntityMgr.findAccountIdsFromRecommendationByLaunchId(launchIds, start, offset, max);
    }

    @Override
    public int getAccountIdsCountFromRecommendationByLaunchId(List<String> launchIds, long start) {
        return recommendationEntityMgr.findAccountIdsCountFromRecommendationByLaunchId(launchIds, start);
    }
}
