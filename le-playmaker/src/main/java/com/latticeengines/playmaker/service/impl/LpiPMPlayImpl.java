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
            playMap.put("name", play.getName());
            playMap.put("pid", play.getPid());
            playMap.put("segmentName", play.getSegmentName());
            playMap.put("LastModificationDate,", play.getLastUpdatedTimestamp());
            result.add(playMap);
        }
        return result;
    }

    @Override
    public int getPlayCount(long start, List<Integer> playgroupIds) {
        List<Play> plays = internalResourceRestApiProxy.getPlays(MultiTenantContext.getCustomerSpace());
        return plays.size();
    }

}
