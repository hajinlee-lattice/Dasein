package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.playmaker.service.LpiPMRecommendation;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMRecommendation")
public class LpiPMRecommendationImpl implements LpiPMRecommendation {

    private static final Logger log = LoggerFactory.getLogger(LpiPMRecommendationImpl.class);

    @Autowired
    private RecommendationEntityMgr recommendationEntityMgr;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public List<Map<String, Object>> getRecommendations(long start, int offset, int maximum,
            SynchronizationDestinationEnum syncDestination, List<String> playIds) {
        return postProcess(recommendationEntityMgr.findRecommendationsAsMap(PlaymakerUtils.dateFromEpochSeconds(start),
                offset, maximum, syncDestination.name(), playIds), offset);
    }

    private List<Map<String, Object>> postProcess(List<Map<String, Object>> data, int offset) {

        List<Play> plays = internalResourceRestApiProxy.getPlays(MultiTenantContext.getCustomerSpace());
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
                                PlayLaunch launch = internalResourceRestApiProxy
                                        .getPlayLaunch(MultiTenantContext.getCustomerSpace(), playName, launchName);
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
                    RuleBucketName bucket = RuleBucketName.valueOf(bucketEnumString);
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
    public int getRecommendationCount(long start, SynchronizationDestinationEnum syncDestination,
            List<String> playIds) {
        return recommendationEntityMgr.findRecommendationCount(PlaymakerUtils.dateFromEpochSeconds(start),
                syncDestination.name(), playIds);
    }

    @VisibleForTesting
    void setRecommendationEntityMgr(RecommendationEntityMgr recommendationEntityMgr) {
        this.recommendationEntityMgr = recommendationEntityMgr;
    }

    @VisibleForTesting
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }
}
