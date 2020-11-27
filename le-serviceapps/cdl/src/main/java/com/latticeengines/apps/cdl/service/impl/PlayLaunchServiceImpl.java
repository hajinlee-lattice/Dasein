package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.LookupIdMapUtils;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.metadata.entitymgr.DataUnitEntityMgr;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;

@Component("playLaunchService")
public class PlayLaunchServiceImpl implements PlayLaunchService {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchServiceImpl.class);

    private static final String NULL_KEY = "NULL_KEY";

    @Value("${aws.customer.export.s3.bucket}")
    private String s3CustomerExportBucket;

    @Value("${aws.customer.s3.bucket}")
    private String s3CustomerBucket;

    @Inject
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Inject
    private DataUnitEntityMgr dataUnitEntityMgr;

    @Override
    public void create(PlayLaunch playLaunch) {
        verifyTablesInLaunch(playLaunch);
        playLaunchEntityMgr.create(playLaunch);
    }

    @Override
    public PlayLaunch findByLaunchId(String launchId, boolean inflate) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        return playLaunchEntityMgr.findByLaunchId(launchId, inflate);
    }

    @Override
    public PlayLaunchChannel findPlayLaunchChannelByLaunchId(String launchId) {
        if (StringUtils.isBlank(launchId)) {
            throw new LedpException(LedpCode.LEDP_18146);
        }
        return playLaunchEntityMgr.findPlayLaunchChannelByLaunchId(launchId);
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
    public PlayLaunch findLatestByChannel(Long playLaunchChannelId) {
        return playLaunchEntityMgr.findLatestByChannel(playLaunchChannelId);
    }

    @Override
    public PlayLaunch findLatestTerminalLaunchByChannel(Long playLaunchChannelId) {
        return playLaunchEntityMgr.findLatestTerminalLaunchByChannel(playLaunchChannelId);
    }

    @Override
    public List<PlayLaunch> findByState(LaunchState state) {
        return playLaunchEntityMgr.findByState(state);
    }

    @Override
    public List<PlayLaunch> findByStateAcrossTenants(LaunchState state, Long max) {
        return playLaunchEntityMgr.findByStateAcrossTenants(state, max);
    }

    @Override
    public PlayLaunch update(PlayLaunch playLaunch) {
        Play play = playLaunch.getPlay();
        if (play != null && Play.TapType.ListSegment.equals(play.getTapType())) {
            verifyDataUnitsInLaunch(playLaunch);
        } else {
            verifyTablesInLaunch(playLaunch);
        }
        playLaunchEntityMgr.update(playLaunch);
        return playLaunchEntityMgr.findByKey(playLaunch);
    }

    private void verifyDataUnitsInLaunch(PlayLaunch playLaunch) {
        if (StringUtils.isNotBlank(playLaunch.getAddAccountsTable())) {
            verifyDataUnitExists(playLaunch.getAddAccountsTable(), playLaunch.getLaunchId(), "AddAccounts");
        }
        if (StringUtils.isNotBlank(playLaunch.getCompleteContactsTable())) {
            verifyDataUnitExists(playLaunch.getCompleteContactsTable(), playLaunch.getLaunchId(), "CompleteContacts");
        }
        if (StringUtils.isNotBlank(playLaunch.getRemoveAccountsTable())) {
            verifyDataUnitExists(playLaunch.getRemoveAccountsTable(), playLaunch.getLaunchId(), "RemoveAccounts");
        }
        if (StringUtils.isNotBlank(playLaunch.getAddContactsTable())) {
            verifyDataUnitExists(playLaunch.getAddContactsTable(), playLaunch.getLaunchId(), "AddContacts");
        }
        if (StringUtils.isNotBlank(playLaunch.getRemoveContactsTable())) {
            verifyDataUnitExists(playLaunch.getRemoveContactsTable(), playLaunch.getLaunchId(), "RemoveContacts");
        }
    }

    private void verifyTablesInLaunch(PlayLaunch playLaunch) {
        if (StringUtils.isNotBlank(playLaunch.getAddAccountsTable())) {
            verifyTableExists(playLaunch.getAddAccountsTable(), playLaunch.getLaunchId(), "AddAccounts");
        }
        if (StringUtils.isNotBlank(playLaunch.getCompleteContactsTable())) {
            verifyTableExists(playLaunch.getCompleteContactsTable(), playLaunch.getLaunchId(), "CompleteContacts");
        }
        if (StringUtils.isNotBlank(playLaunch.getRemoveAccountsTable())) {
            verifyTableExists(playLaunch.getRemoveAccountsTable(), playLaunch.getLaunchId(), "RemoveAccounts");
        }
        if (StringUtils.isNotBlank(playLaunch.getAddContactsTable())) {
            verifyTableExists(playLaunch.getAddContactsTable(), playLaunch.getLaunchId(), "AddContacts");
        }
        if (StringUtils.isNotBlank(playLaunch.getRemoveContactsTable())) {
            verifyTableExists(playLaunch.getRemoveContactsTable(), playLaunch.getLaunchId(), "RemoveContacts");
        }
    }

    private String verifyTableExists(String tableName, String launchId, String tag) {
        Table table = tableEntityMgr.findByName(tableName, false, false);
        if (table != null) {
            return table.getName();
        } else {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Failed to update Launch: " + launchId + " since no " + tag + " table found by Id: " + tableName });
        }
    }

    private String verifyDataUnitExists(String dataUnitName, String launchId, String tag) {
        List<DataUnit> dataUnits = dataUnitEntityMgr.findByNameFromReader(MultiTenantContext.getShortTenantId(), dataUnitName);
        if (CollectionUtils.isEmpty(dataUnits)) {
            return dataUnitName;
        } else {
            throw new LedpException(LedpCode.LEDP_32000, new String[]{
                    "Failed to update Launch: " + launchId + " since no " + tag + " table found by Id: " + dataUnitName});
        }
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

        Map<String, List<LookupIdMap>> uniqueLookupIdMaps = calculateUniqueLookupIdMapping(playId, launchStates,
                startTimestamp, endTimestamp, orgId, externalSysType, skipLoadingAllLookupIdMapping);
        Map<String, LookupIdMap> lookupIdMaps = uniqueLookupIdMaps.values().stream().flatMap(Collection::stream)
                .collect(Collectors.toMap(LookupIdMap::getOrgId, Function.identity()));

        addDataIntegrationStatusFor(launchSummaries, lookupIdMaps);
        dashboard.setLaunchSummaries(launchSummaries);
        dashboard.setCumulativeStats(totalCounts);
        dashboard.setUniquePlaysWithLaunches(uniquePlaysWithLaunches);
        dashboard.setUniqueLookupIdMapping(uniqueLookupIdMaps);
        return dashboard;
    }

    private void addDataIntegrationStatusFor(List<LaunchSummary> launchSummaries,
            Map<String, LookupIdMap> lookupIdMaps) {
        if (CollectionUtils.isEmpty(launchSummaries)) {
            return;
        }
        List<String> launchIds = launchSummaries.stream().map(LaunchSummary::getLaunchId)
                .filter(StringUtils::isNotBlank).collect(Collectors.toList());
        List<DataIntegrationStatusMonitor> dataIntegrationStatusMonitors = dataIntegrationStatusMonitoringEntityMgr
                .getAllStatusesByEntityNameAndIds(MultiTenantContext.getTenant().getPid(), "PlayLaunch", launchIds);
        log.debug("For given {} PlayLaunch objects, {} DataIntegrationStatus objects found", launchIds.size(),
                dataIntegrationStatusMonitors.size());
        if (CollectionUtils.isEmpty(dataIntegrationStatusMonitors)) {
            return;
        }

        Map<String, DataIntegrationStatusMonitor> dataIntegrationStatusMap = dataIntegrationStatusMonitors.stream()
                .collect(Collectors.toMap(DataIntegrationStatusMonitor::getEntityId, dism -> dism));
        launchSummaries.forEach(ls -> ls.setIntegrationStatusMonitor(dataIntegrationStatusMap.get(ls.getLaunchId())));
        launchSummaries.forEach(ls -> {
            if (ls.getIntegrationStatusMonitor() != null) {
                LookupIdMap lookupIdMap = lookupIdMaps.get(ls.getDestinationOrgId());
                ls.getIntegrationStatusMonitor()
                        .setS3Bucket(lookupIdMap.isTrayEnabled() ? s3CustomerExportBucket : s3CustomerBucket);
            }
        });
    }

    @Override
    public Long getDashboardEntriesCount(Long playId, List<LaunchState> launchStates, Long startTimestamp,
            Long endTimestamp, String orgId, String externalSysType) {
        return playLaunchEntityMgr.findDashboardEntriesCount(playId, launchStates, startTimestamp, endTimestamp, orgId,
                externalSysType);
    }

    @Override
    public PlayLaunch updateAudience(String audienceId, String audienceName, String playLaunchId) {
        if (playLaunchId != null) {
            PlayLaunch playLaunch = playLaunchEntityMgr.findByLaunchId(playLaunchId, false);
            playLaunch.setAudienceId(audienceId);
            playLaunch.setAudienceName(audienceName);
            ChannelConfig channelConfig = playLaunch.getChannelConfig();
            channelConfig.setAudienceId(audienceId);
            channelConfig.setAudienceName(audienceName);
            playLaunch.setChannelConfig(channelConfig);
            playLaunchEntityMgr.update(playLaunch);
            playLaunchChannelService.updateAudience(audienceId, audienceName, playLaunchId);
            return playLaunch;
        }
        throw new LedpException(LedpCode.LEDP_18236);
    }

    private Map<String, List<LookupIdMap>> calculateUniqueLookupIdMapping(Long playId, List<LaunchState> launchStates,
            Long startTimestamp, Long endTimestamp, String orgId, String externalSysType,
            boolean skipLoadingAllLookupIdMapping) {
        Map<String, List<LookupIdMap>> allLookupIdMapping = skipLoadingAllLookupIdMapping ? null
                : LookupIdMapUtils.listToMap(lookupIdMappingEntityMgr.getLookupIdMappings(null, null, true));

        List<Pair<String, String>> uniqueOrgIdList = playLaunchEntityMgr.findDashboardOrgIdWithLaunches(playId,
                launchStates, startTimestamp, endTimestamp, orgId, externalSysType);
        return

        mergeLookupIdMapping(allLookupIdMapping, uniqueOrgIdList);
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
                    .forEach(k -> allLookupIdMapping.get(k).stream().filter(mapping -> uniqueOrgIdSet //
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
            uniqueOrgIdSet //
                    .forEach(org -> {
                        String orgId = org.getLeft();
                        String externalSysType = org.getRight();
                        LookupIdMap lookupIdMap = new LookupIdMap();
                        String key;
                        CDLExternalSystemType externalSystemTypeEnum;

                        if (StringUtils.isBlank(externalSysType)
                                || !EnumUtils.isValidEnum(CDLExternalSystemType.class, externalSysType)) {
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
}
