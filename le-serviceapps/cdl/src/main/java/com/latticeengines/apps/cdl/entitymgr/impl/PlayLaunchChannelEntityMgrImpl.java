package com.latticeengines.apps.cdl.entitymgr.impl;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PlayLaunchChannelDao;
import com.latticeengines.apps.cdl.entitymgr.LookupIdMappingEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchChannelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.repository.PlayLaunchChannelRepository;
import com.latticeengines.apps.cdl.repository.reader.PlayLaunchChannelReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.PlayLaunchChannelWriterRepository;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.metadata.entitymgr.TableEntityMgr;
import com.latticeengines.proxy.exposed.quartz.QuartzSchedulerProxy;

@Component("playLaunchChannelEntityMgr")
public class PlayLaunchChannelEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<PlayLaunchChannelRepository, PlayLaunchChannel, Long> //
        implements PlayLaunchChannelEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchChannelEntityMgrImpl.class);

    @Value("${cdl.channel.maximum.expiration.month}")
    private Long maxExpirationMonths;

    @Inject
    private PlayLaunchChannelDao playLaunchChannelDao;

    @Inject
    private PlayLaunchChannelEntityMgrImpl _self;

    @Inject
    private QuartzSchedulerProxy quartzSchedulerProxy;

    @Inject
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private LookupIdMappingEntityMgr lookupIdMappingEntityMgr;

    @Inject
    private TableEntityMgr tableEntityMgr;

    @Resource(name = "playLaunchChannelWriterRepository")
    private PlayLaunchChannelWriterRepository writerRepository;

    @Resource(name = "playLaunchChannelReaderRepository")
    private PlayLaunchChannelReaderRepository readerRepository;

    @Override
    protected PlayLaunchChannelRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected PlayLaunchChannelRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected PlayLaunchChannelEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    public BaseDao<PlayLaunchChannel> getDao() {
        return playLaunchChannelDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunchChannel> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunchChannel> findByIsAlwaysOnTrue() {
        return readerRepository.findByIsAlwaysOnTrue();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlayLaunchChannel> findByPlayName(String playName) {
        return readerRepository.findByPlayName(playName);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayLaunchChannel findByPlayNameAndLookupIdMapId(String playName, String lookupId) {
        return readerRepository.findByPlayNameAndLookupIdMapId(playName, lookupId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlayLaunchChannel findById(String channelId) {
        return readerRepository.findById(channelId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public PlayLaunchChannel createPlayLaunchChannel(PlayLaunchChannel playLaunchChannel) {
        if (playLaunchChannel.getLookupIdMap() == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot create a channel without a valid LookupIdMap" });
        }
        if (StringUtils.isBlank(playLaunchChannel.getLookupIdMap().getId())) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Id cannot be empty for the provided LookupIdMap" });
        }
        LookupIdMap lookupIdMap = lookupIdMappingEntityMgr.getLookupIdMap(playLaunchChannel.getLookupIdMap().getId());
        if (lookupIdMap == null) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "No lookupIdMap found by Id: " + playLaunchChannel.getLookupIdMap().getId() });
        }
        verifyChannelConfigHasSameDestinationAsLookupIdMap(lookupIdMap, playLaunchChannel);
        if (playLaunchChannel.getIsAlwaysOn() != null && playLaunchChannel.getIsAlwaysOn()
                && validateAlwaysOnExpiration(playLaunchChannel)) {
            playLaunchChannel
                    .setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(playLaunchChannel));
            playLaunchChannel.setExpirationDate(
                    PlayLaunchChannel.getExpirationDateFromExpirationPeriodString(playLaunchChannel));
        }
        playLaunchChannel.setLookupIdMap(lookupIdMap);
        playLaunchChannel.setId(playLaunchChannel.generateChannelId());
        playLaunchChannelDao.create(playLaunchChannel);
        return playLaunchChannel;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public PlayLaunchChannel updatePlayLaunchChannel(PlayLaunchChannel existingPlayLaunchChannel,
            PlayLaunchChannel updatedChannel) {
        if (updatedChannel.getMaxAccountsToLaunch() != null) {
            existingPlayLaunchChannel.setMaxAccountsToLaunch(updatedChannel.getMaxAccountsToLaunch());
        }
        if (updatedChannel.getBucketsToLaunch() != null) {
            existingPlayLaunchChannel.setBucketsToLaunch(updatedChannel.getBucketsToLaunch());
        }
        if (updatedChannel.isLaunchUnscored()) {
            existingPlayLaunchChannel.setLaunchUnscored(updatedChannel.isLaunchUnscored());
        }
        if (updatedChannel.getLaunchType() != null) {
            existingPlayLaunchChannel.setLaunchType(updatedChannel.getLaunchType());
        }

        // When always-on is off and was not changed
        if (updatedChannel.getIsAlwaysOn() != null && !updatedChannel.getIsAlwaysOn()
                && !existingPlayLaunchChannel.getIsAlwaysOn()) {
            // Do nothing
        }
        // when always-on is being turned off from on
        else if (updatedChannel.getIsAlwaysOn() != null && !updatedChannel.getIsAlwaysOn()
                && existingPlayLaunchChannel.getIsAlwaysOn()) {
            existingPlayLaunchChannel.setIsAlwaysOn(updatedChannel.getIsAlwaysOn());
            // Setting null here so that when user explicitly turns always-on off, there is
            // no expiration date
            existingPlayLaunchChannel.setExpirationDate(null);
            existingPlayLaunchChannel.setNextScheduledLaunch(null);
        }
        // When always-on is being turned on from off
        else if (updatedChannel.getIsAlwaysOn() != null && updatedChannel.getIsAlwaysOn()
                && !existingPlayLaunchChannel.getIsAlwaysOn()) {
            validateAlwaysOnExpiration(updatedChannel);
            existingPlayLaunchChannel.setIsAlwaysOn(updatedChannel.getIsAlwaysOn());
            existingPlayLaunchChannel
                    .setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(updatedChannel));
            existingPlayLaunchChannel
                    .setExpirationDate(PlayLaunchChannel.getExpirationDateFromExpirationPeriodString(updatedChannel));
        }
        // When always-on is on and not changed, but scheduling or expiry could have
        // changed
        else if (updatedChannel.getIsAlwaysOn() != null && updatedChannel.getIsAlwaysOn()
                && existingPlayLaunchChannel.getIsAlwaysOn()) {
            if (StringUtils.isNotBlank(updatedChannel.getCronScheduleExpression()) && !updatedChannel
                    .getCronScheduleExpression().equals(existingPlayLaunchChannel.getCronScheduleExpression())) {
                existingPlayLaunchChannel.setCronScheduleExpression(updatedChannel.getCronScheduleExpression());
                existingPlayLaunchChannel
                        .setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(updatedChannel));
            }
            if (StringUtils.isNotBlank(updatedChannel.getExpirationPeriodString())
                    && !updatedChannel.getExpirationPeriodString()
                            .equals(existingPlayLaunchChannel.getExpirationPeriodString())
                    && validateAlwaysOnExpiration(updatedChannel)) {
                existingPlayLaunchChannel.setExpirationDate(
                        PlayLaunchChannel.getExpirationDateFromExpirationPeriodString(updatedChannel));
            }

        }

        if (updatedChannel.getChannelConfig() != null) {
            if (updatedChannel.getLookupIdMap() == null) {
                throw new LedpException(LedpCode.LEDP_32000,
                        new String[] { "Cannot create a channel without a valid LookupIdMap" });
            }
            if (StringUtils.isBlank(updatedChannel.getLookupIdMap().getId())) {
                throw new LedpException(LedpCode.LEDP_32000,
                        new String[] { "Id cannot be empty for the provided LookupIdMap" });
            }
            LookupIdMap lookupIdMap = lookupIdMappingEntityMgr
                    .getLookupIdMap(existingPlayLaunchChannel.getLookupIdMap().getId());
            if (lookupIdMap == null) {
                throw new LedpException(LedpCode.LEDP_32000, new String[] {
                        "No lookupIdMap found by Id: " + existingPlayLaunchChannel.getLookupIdMap().getId() });
            }
            verifyChannelConfigHasSameDestinationAsLookupIdMap(lookupIdMap, updatedChannel);

            if (existingPlayLaunchChannel.getChannelConfig() != null) {
                existingPlayLaunchChannel.setChannelConfig(
                        existingPlayLaunchChannel.getChannelConfig().copyConfig(updatedChannel.getChannelConfig()));
            } else {
                existingPlayLaunchChannel.setChannelConfig(updatedChannel.getChannelConfig());
            }
        }
        if (updatedChannel.getLastLaunch() != null) {
            existingPlayLaunchChannel.setLastLaunch(updatedChannel.getLastLaunch());
        }
        if (StringUtils.isNotBlank(updatedChannel.getCurrentLaunchedAccountUniverseTable())) {
            Table table = tableEntityMgr.findByName(updatedChannel.getCurrentLaunchedAccountUniverseTable(), false,
                    false);
            if (table != null) {
                existingPlayLaunchChannel.setCurrentLaunchedAccountUniverseTable(table.getName());
            } else {
                throw new LedpException(LedpCode.LEDP_32000,
                        new String[] { "Failed to update channel: " + updatedChannel.getId()
                                + " since no account universe table found by Id: "
                                + updatedChannel.getCurrentLaunchedAccountUniverseTable() });
            }
        }
        if (StringUtils.isNotBlank(updatedChannel.getCurrentLaunchedContactUniverseTable())) {
            Table table = tableEntityMgr.findByName(updatedChannel.getCurrentLaunchedContactUniverseTable(), false,
                    false);
            if (table != null) {
                existingPlayLaunchChannel.setCurrentLaunchedContactUniverseTable(table.getName());
            } else {
                throw new LedpException(LedpCode.LEDP_32000,
                        new String[] { "Failed to update channel: " + updatedChannel.getId()
                                + " since no contact universe table found by Id: "
                                + updatedChannel.getCurrentLaunchedAccountUniverseTable() });
            }
        }
        existingPlayLaunchChannel.setUpdatedBy(updatedChannel.getUpdatedBy());

        playLaunchChannelDao.update(existingPlayLaunchChannel);
        return existingPlayLaunchChannel;
    }

    @Override
    @Modifying
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByChannelId(String id, boolean hardDelete) {
        PlayLaunchChannel playLaunchChannel = findById(id);
        if (playLaunchChannel != null) {
            if (hardDelete) {
                playLaunchChannelDao.delete(playLaunchChannel);
            } else {
                playLaunchChannel.setDeleted(true);
                playLaunchChannelDao.update(playLaunchChannel);
            }
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public List<PlayLaunchChannel> getAllValidScheduledChannels() {
        List<PlayLaunchChannel> channels = readerRepository.findAlwaysOnChannelsByNextScheduledTime(
                new Date(Long.MIN_VALUE), DateUtils.addMinutes(new Date(), 15));
        channels.forEach(c -> {
            Hibernate.initialize(c.getTenant());
            Hibernate.initialize(c.getPlay());
        });
        return channels;
    }

    private boolean validateAlwaysOnExpiration(PlayLaunchChannel channel) {
        if (!channel.getIsAlwaysOn())
            return true;

        if (StringUtils.isBlank(channel.getCronScheduleExpression())) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Need a Cron Schedule Expression if a Channel is Always On" });
        }
        if (StringUtils.isBlank(channel.getExpirationPeriodString())) {
            // TODO: PLS-14902: Uncomment once UI is ready
            // throw new LedpException(LedpCode.LEDP_32000,
            // new String[] { "Need an expiration period if a Channel is Always On" });
        }
        Date expirationDate = PlayLaunchChannel.getExpirationDateFromExpirationPeriodString(channel);
        if (Instant.now().atOffset(ZoneOffset.UTC).plusMonths(maxExpirationMonths)
                .isBefore(expirationDate.toInstant().atOffset(ZoneOffset.UTC))) {
            throw new LedpException(LedpCode.LEDP_18232,
                    new String[] { channel.getExpirationDate().toString(), maxExpirationMonths.toString() });
        }
        return true;
    }

    private void verifyChannelConfigHasSameDestinationAsLookupIdMap(LookupIdMap lookupIdMap,
            PlayLaunchChannel playLaunchChannel) {
        CDLExternalSystemName systemName = lookupIdMap.getExternalSystemName();
        if (!(playLaunchChannel.getChannelConfig().getSystemName().equals(systemName))) {
            throw new LedpException(LedpCode.LEDP_18222,
                    new String[] { JsonUtils.serialize(playLaunchChannel.getChannelConfig()).split("\"")[1],
                            systemName.getDisplayName() });
        }
    }

}
