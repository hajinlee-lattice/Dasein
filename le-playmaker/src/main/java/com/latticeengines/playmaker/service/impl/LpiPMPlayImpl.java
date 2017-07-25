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

import com.latticeengines.domain.exposed.pls.Play;
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
                playMap.put("ID", play.getPid());
                playMap.put("ExternalId", play.getName());
                playMap.put("DisplayName", play.getDisplayName());
                playMap.put("Description", play.getDescription());
                playMap.put("AverageProbability", dummyAvgProbability());
                playMap.put("LastModificationDate", secondsFromEpoch(play));
                playMap.put("PlayGroups", dummyPlayGroups());
                playMap.put("TargetProducts", dummyTargetProducts());
                playMap.put("Workflow", dummyWorkfowType());
                result.add(playMap);
            }
        }
        return result;
    }

    private String dummyWorkfowType() {
        return "WfTypeId1";
    }

    private String dummyTargetProducts() {
        return "Wireless Router|productId1";
    }

    private String dummyPlayGroups() {
        return "PG1|PG2";
    }

    private String dummyAvgProbability() {
        return "0.5";
    }

    private long secondsFromEpoch(Play play) {
        try {
            return play.getLastUpdatedTimestamp().getTime() / 1000;
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

}
