package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchConfigurations;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.LaunchSummary;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;

@Component("playLaunchService")
public class PlayLaunchServiceImpl implements PlayLaunchService {

    private static final String NULL_KEY = "NULL_KEY";

    @Inject
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Override
    public void create(PlayLaunch entity) {
        playLaunchEntityMgr.create(entity);
    }

    @Override
    public PlayLaunch findByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        return playLaunchEntityMgr.findByLaunchId(launchId);
    }

    @Override
    public void deleteByLaunchId(String launchId, boolean hardDelete) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        playLaunchEntityMgr.deleteByLaunchId(launchId, hardDelete);
    }

    @Override
    public PlayLaunch findByPlayAndTimestamp(Long playId, Date timestamp) {
        return playLaunchEntityMgr.findByPlayAndTimestamp(playId, timestamp);
    }

    @Override
    public List<PlayLaunch> findByPlayId(Long playId, List<LaunchState> states) {
        return playLaunchEntityMgr.findByPlayId(playId, states);
    }

    @Override
    public PlayLaunch findLatestByPlayId(Long playId, List<LaunchState> launchStates) {
        return playLaunchEntityMgr.findLatestByPlayId(playId, launchStates);
    }

    @Override
    public PlayLaunch findLatestByPlayAndSysOrg(Long playId, String orgId) {
        return playLaunchEntityMgr.findLatestByPlayAndSysOrg(playId, orgId);
    }

    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        return playLaunchEntityMgr.findByState(state);
    }

    @Override
    public PlayLaunch update(PlayLaunch playLaunch) {
        playLaunchEntityMgr.update(playLaunch);
        return playLaunchEntityMgr.findByKey(playLaunch);
    }

    @Override
    public PlayLaunchDashboard getDashboard(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long offset, Long max, String sortby, boolean descending, Long endTimestamp, String orgId,
            String externalSysType, boolean skipLoadingAllLookupIdMapping) {
        PlayLaunchDashboard dashboard = new PlayLaunchDashboard();
        Stats totalCounts = playLaunchEntityMgr.findDashboardCumulativeStats(playId, launchStates, startTimestamp,
                endTimestamp, orgId, externalSysType);

        List<Play> uniquePlaysWithLaunches = playLaunchEntityMgr.findDashboardPlaysWithLaunches(playId, launchStates,
                startTimestamp, endTimestamp, orgId, externalSysType);

        List<LaunchSummary> launchSummaries = playLaunchEntityMgr.findDashboardEntries(playId, launchStates,
                startTimestamp, offset, max, sortby, descending, endTimestamp, orgId, externalSysType);

        dashboard.setLaunchSummaries(launchSummaries);
        dashboard.setCumulativeStats(totalCounts);
        dashboard.setUniquePlaysWithLaunches(uniquePlaysWithLaunches);
        dashboard.setUniqueLookupIdMapping(calculateUniqueLookupIdMapping(playId, launchStates, startTimestamp,
                endTimestamp, orgId, externalSysType, skipLoadingAllLookupIdMapping));
        return dashboard;
    }

    @Override
    public Long getDashboardEntriesCount(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        return playLaunchEntityMgr.findDashboardEntriesCount(playId, launchStates, startTimestamp, endTimestamp, orgId,
                externalSysType);
    }

    private Map<String, List<LookupIdMap>> calculateUniqueLookupIdMapping(Long playId, List<LaunchState> launchStates,
            Long startTimestamp, Long endTimestamp, String orgId, String externalSysType,
            boolean skipLoadingAllLookupIdMapping) {
        Tenant tenant = MultiTenantContext.getTenant();
        Map<String, List<LookupIdMap>> allLookupIdMapping = skipLoadingAllLookupIdMapping ? null
                : lookupIdMappingProxy.getLookupIdsMapping(tenant.getId(), null, null, true);
        List<Pair<String, String>> uniqueOrgIdList = playLaunchEntityMgr.findDashboardOrgIdWithLaunches(playId,
                launchStates, startTimestamp, endTimestamp, orgId, externalSysType);
        Map<String, List<LookupIdMap>> uniqueLookupIdMapping =
                mergeLookupIdMapping(allLookupIdMapping, uniqueOrgIdList);
        return uniqueLookupIdMapping;
    }

    @VisibleForTesting
    Map<String, List<LookupIdMap>> mergeLookupIdMapping(Map<String, List<LookupIdMap>> allLookupIdMapping,
            List<Pair<String, String>> uniqueOrgIdList) {
        // logic is to fist find unique list of org Ids. Then get all existing
        // mappings. Then retain only those existing mappings for which org id
        // is present in first list of org ids.
        // If there are any org ids which are not present in existing mapping
        // (corner case), simply create new LookupIdMap object with known
        // details and add them to the list
        Set<Pair<String, String>> uniqueOrgIdSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(uniqueOrgIdList)) {
            uniqueOrgIdSet.addAll(uniqueOrgIdList);
        }

        Map<String, List<LookupIdMap>> uniqueLookupIdMapping = new HashMap<>();
        if (MapUtils.isNotEmpty(allLookupIdMapping)) {
            allLookupIdMapping.keySet().stream() //
                    .filter(k -> CollectionUtils.isNotEmpty(allLookupIdMapping.get(k))) //
                    .forEach(k -> allLookupIdMapping.get(k).stream()
                            .filter(mapping -> uniqueOrgIdSet //
                                    .contains(new ImmutablePair<>(mapping.getOrgId(), k))) //
                            .forEach(mapping -> {
                                if (!uniqueLookupIdMapping.containsKey(k)) {
                                    uniqueLookupIdMapping.put(k, new ArrayList<>());
                                }
                                uniqueLookupIdMapping.get(k).add(mapping);

                                // remove this from unique set so that at the
                                // end of thos stream processing we'll no orphan
                                // org info
                                uniqueOrgIdSet.remove(new ImmutablePair<>(mapping.getOrgId(), k));
                            }));
        }

        // handle orphan orgs
        if (CollectionUtils.isNotEmpty(uniqueOrgIdSet)) {
            uniqueOrgIdSet.stream() //
                    .forEach(org -> {
                        String orgId = org.getLeft();
                        String externalSysType = org.getRight();
                        LookupIdMap lookupIdMap = new LookupIdMap();
                        String key = null;
                        CDLExternalSystemType externalSystemTypeEnum;

                        if (StringUtils.isBlank(externalSysType)
                                || CDLExternalSystemType.valueOf(externalSysType) == null) {
                            key = NULL_KEY;
                            externalSystemTypeEnum = null;
                        } else {
                            key = externalSysType;
                            externalSystemTypeEnum = CDLExternalSystemType.valueOf(externalSysType);
                        }
                        if (!uniqueLookupIdMapping.containsKey(key)) {
                            uniqueLookupIdMapping.put(key, new ArrayList<>());
                        }
                        lookupIdMap.setOrgId(orgId);
                        lookupIdMap.setExternalSystemType(externalSystemTypeEnum);
                        uniqueLookupIdMapping.get(key).add(lookupIdMap);
                    });
        }
        return uniqueLookupIdMapping;
    }


    @Override
    public PlayLaunchConfigurations getPlayLaunchConfigurations(Long playId) {
        PlayLaunchConfigurations configurations = new PlayLaunchConfigurations();
        Tenant tenant = MultiTenantContext.getTenant();
        Map<String, List<LookupIdMap>> allLookupIdMapping =
                lookupIdMappingProxy.getLookupIdsMapping(tenant.getId(), null, null, true);
        configurations.setUniqueLookupIdMapping(allLookupIdMapping);
        configurations.setLaunchConfigurations(createLaunchConfigurationMap(playId, allLookupIdMapping));
        return configurations;
    }

    private Map<String, PlayLaunch> createLaunchConfigurationMap(Long playId,
            Map<String, List<LookupIdMap>> allLookupIdMapping) {
        Map<String, PlayLaunch> configurationMap = new HashMap<>();

        if (MapUtils.isNotEmpty(allLookupIdMapping)) {
            allLookupIdMapping.keySet().stream() //
                    .filter(k -> CollectionUtils.isNotEmpty(allLookupIdMapping.get(k))) //
                    .forEach(k -> allLookupIdMapping.get(k).stream().forEach(mapping -> {
                        String orgId = mapping.getOrgId();
                        configurationMap.put(orgId, findLatestByPlayAndSysOrg(playId, orgId));
                    }));
        }

        return configurationMap;
    }
}
