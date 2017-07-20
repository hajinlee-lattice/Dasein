package com.latticeengines.playmaker.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.playmaker.service.LpiPMPlay;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("lpiPMPlay")
public class LpiPMPlayImpl implements LpiPMPlay {

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

        for (Play play : plays) {
            Map<String, Object> playMap = new HashMap<>();
            playMap.put("Id", play.getName());
            playMap.put("ExternalId", play.getName());
            playMap.put("DisplayName", play.getDisplayName());
            playMap.put("Description", play.getDescription());
            playMap.put("AverageProbability", dummyAvgProbability());
            playMap.put("LastModificationDate,", secondsFromEpoch(play));
            playMap.put("PlayGroups,", dummyPlayGroups());
            playMap.put("TargetProducts,", dummyTargetProducts());
            playMap.put("Workflow,", dummyWorkfowType());
            result.add(playMap);
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
        return play.getLastUpdatedTimestamp().getTime() / 1000;
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        List<Play> plays = internalResourceRestApiProxy.getPlays(MultiTenantContext.getCustomerSpace());
        return plays.size();
    }

}
