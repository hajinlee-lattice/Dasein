package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMPlay")
public class LpiPMPlayImpl implements LpiPMPlay {

    private static final Logger log = LoggerFactory.getLogger(LpiPMPlayImpl.class);

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @PostConstruct
    public void init() {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public List<Map<String, Object>> getPlays(long start, int offset, int maximum, List<Integer> playgroupIds) {

        List<Play> plays = internalResourceRestApiProxy.getPlays(MultiTenantContext.getCustomerSpace());

        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        int skipped = 0;
        int rowNum = offset + 1;

        for (Play play : plays) {
            // TODO - implement this pagination in play dao impl and expose it
            // via play resource
            if (secondsFromEpoch(play) >= start) {
                if (skipped < offset) {
                    skipped++;
                    continue;
                }

                if (result.size() >= maximum) {
                    break;
                }

                Map<String, Object> playMap = new HashMap<>();
                playMap.put(PlaymakerConstants.ID, play.getPid());
                playMap.put(PlaymakerConstants.ID + PlaymakerConstants.V2, play.getName());
                playMap.put(PlaymakerConstants.ExternalId, play.getName());
                playMap.put(PlaymakerConstants.DisplayName, play.getDisplayName());
                playMap.put(PlaymakerConstants.Description, play.getDescription());
                playMap.put(PlaymakerConstants.AverageProbability, dummyAvgProbability());
                playMap.put(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY, secondsFromEpoch(play));
                playMap.put(PlaymakerConstants.PlayGroups, dummyPlayGroups());
                playMap.put(PlaymakerConstants.TargetProducts, dummyTargetProducts());
                playMap.put(PlaymakerConstants.Workflow, dummyWorkfowType());
                playMap.put(PlaymakerConstants.RowNum, rowNum++);
                result.add(playMap);
            }
        }
        return result;
    }

    private String dummyWorkfowType() {
        return PlaymakerConstants.DefaultWorkflowType;
    }

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

    private String dummyPlayGroups() {
        return null;// "[PG2]";
    }

    private String dummyAvgProbability() {
        return null;// "0.5";
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
    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy2) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy2;
    }

}
