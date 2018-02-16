package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

@Component("lpiPMPlay")
public class LpiPMPlayImpl implements LpiPMPlay {

    private static final Logger log = LoggerFactory.getLogger(LpiPMPlayImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private PlayProxy playProxy;

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {
        // following API has sub-second performance even for large number of
        // plays (50+). This API returns only those plays for with there is
        // at least one play launch. Implementing pagination in this API may not
        // be worth the effort as it is fast enough. Therefore handling
        // pagination in application layer itself
        PlayLaunchDashboard dashboard = playProxy.getPlayLaunchDashboard(
                MultiTenantContext.getCustomerSpace().toString(), null, null, 0L, 0L, 1L, null, null, null);
        List<Play> plays = dashboard.getUniquePlaysWithLaunches();

        return handlePagination(start, offset, maximum, plays);
    }

    private List<Map<String, Object>> handlePagination(long start, int offset, int maximum, List<Play> plays) {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        int skipped = 0;
        int rowNum = offset + 1;

        for (Play play : plays) {
            if (secondsFromEpoch(play) >= start) {
                if (skipped < offset) {
                    skipped++;
                    continue;
                }
                if (result.size() >= maximum) {
                    break;
                }
                generatePlayObjForSync(result, rowNum++, play);
            }
        }
        return result;
    }

    private Map<String, Object> generatePlayObjForSync(List<Map<String, Object>> result, int rowNum, Play play) {
        Map<String, Object> playMap = new HashMap<>();
        playMap.put(PlaymakerConstants.ID, play.getPid());
        playMap.put(PlaymakerConstants.ID + PlaymakerConstants.V2, play.getName());
        playMap.put(PlaymakerConstants.ExternalId, play.getName());
        playMap.put(PlaymakerConstants.DisplayName, play.getDisplayName());
        playMap.put(PlaymakerConstants.Description, play.getDescription());
        playMap.put(PlaymakerConstants.AverageProbability, null);
        playMap.put(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY, secondsFromEpoch(play));
        playMap.put(PlaymakerConstants.PlayGroups, null);
        playMap.put(PlaymakerConstants.TargetProducts, dummyTargetProducts());
        playMap.put(PlaymakerConstants.Workflow, dummyWorkfowType());
        playMap.put(PlaymakerConstants.RowNum, rowNum);
        result.add(playMap);
        return playMap;
    }

    // TODO - remove following when we have workflow type defined for plays
    private String dummyWorkfowType() {
        return PlaymakerConstants.DefaultWorkflowType;
    }

    // TODO - remove following when we have products associated with plays
    private List<Map<String, String>> dummyTargetProducts() {
        List<Map<String, String>> products = new ArrayList<>();
        Map<String, String> prod1 = new HashMap<>();
        prod1.put(PlaymakerConstants.DisplayName, "Series K Disk");
        prod1.put(PlaymakerConstants.ExternalName, "NETSUITE-SRSK");
        products.add(prod1);
        Map<String, String> prod2 = new HashMap<>();
        prod2.put(PlaymakerConstants.DisplayName, "Series L Disk");
        prod2.put(PlaymakerConstants.ExternalName, "NETSUITE-SRSL");
        products.add(prod2);
        return products;
    }

    private long secondsFromEpoch(Play play) {
        try {
            return play.getUpdated().getTime() / 1000;
        } catch (Exception ex) {
            log.error("Ignoring this error", ex);
            return 0L;
        }
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        List<Map<String, Object>> plays = getPlays(start, 0, Integer.MAX_VALUE, null);
        return plays.size();
    }

    @VisibleForTesting
    void setPlayProxy(PlayProxy playProxy) {
        this.playProxy = playProxy;
    }

}
