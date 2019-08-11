package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.time.DateUtils;
import org.hibernate.Hibernate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.proxy.exposed.quartz.QuartzSchedulerProxy;

@Component("playLaunchChannelEntityMgr")
public class PlayLaunchChannelEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<PlayLaunchChannelRepository, PlayLaunchChannel, Long> //
        implements PlayLaunchChannelEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchChannelEntityMgrImpl.class);

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
        verifyNewPlayLaunchChannel(playLaunchChannel);
        if (playLaunchChannel.getCronScheduleExpression() != null) {
            playLaunchChannel
                    .setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(playLaunchChannel));
        }
        playLaunchChannelDao.create(playLaunchChannel);
        return playLaunchChannel;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public PlayLaunchChannel updatePlayLaunchChannel(PlayLaunchChannel existingPlayLaunchChannel,
            PlayLaunchChannel playLaunchChannel) {
        if (playLaunchChannel.getIsAlwaysOn() != null) {
            existingPlayLaunchChannel.setIsAlwaysOn(playLaunchChannel.getIsAlwaysOn());
        }
        if (playLaunchChannel.getMaxAccountsToLaunch() != null) {
            existingPlayLaunchChannel.setMaxAccountsToLaunch(playLaunchChannel.getMaxAccountsToLaunch());
        }
        if (playLaunchChannel.getBucketsToLaunch() != null) {
            existingPlayLaunchChannel.setBucketsToLaunch(playLaunchChannel.getBucketsToLaunch());
        }
        if (playLaunchChannel.isLaunchUnscored()) {
            existingPlayLaunchChannel.setLaunchUnscored(playLaunchChannel.isLaunchUnscored());
        }
        if (playLaunchChannel.getLaunchType() != null) {
            existingPlayLaunchChannel.setLaunchType(playLaunchChannel.getLaunchType());
        }
        if (playLaunchChannel.getNextScheduledLaunch() != null) {
            existingPlayLaunchChannel.setNextScheduledLaunch(playLaunchChannel.getNextScheduledLaunch());
        }
        if (playLaunchChannel.getCronScheduleExpression() != null && !playLaunchChannel.getCronScheduleExpression()
                .equals(existingPlayLaunchChannel.getCronScheduleExpression())) {
            existingPlayLaunchChannel.setCronScheduleExpression(playLaunchChannel.getCronScheduleExpression());
            existingPlayLaunchChannel
                    .setNextScheduledLaunch(PlayLaunchChannel.getNextDateFromCronExpression(existingPlayLaunchChannel));
        }
        if (playLaunchChannel.getChannelConfig() != null) {
            LookupIdMap lookupIdMap = findLookupIdMap(playLaunchChannel);
            verifyChannelConfigHasSameDestinationAsLookupIdMap(lookupIdMap, playLaunchChannel);
            if (existingPlayLaunchChannel.getChannelConfig() != null) {
                existingPlayLaunchChannel.setChannelConfig(
                        existingPlayLaunchChannel.getChannelConfig().copyConfig(playLaunchChannel.getChannelConfig()));
            } else {
                existingPlayLaunchChannel.setChannelConfig(playLaunchChannel.getChannelConfig());
            }
        }
        if (playLaunchChannel.getCurrentLaunchedAccountUniverseTable() != null) {
            existingPlayLaunchChannel
                    .setCurrentLaunchedAccountUniverseTable(playLaunchChannel.getCurrentLaunchedAccountUniverseTable());
        }
        if (playLaunchChannel.getCurrentLaunchedContactUniverseTable() != null) {
            existingPlayLaunchChannel
                    .setCurrentLaunchedContactUniverseTable(playLaunchChannel.getCurrentLaunchedContactUniverseTable());
        }
        existingPlayLaunchChannel.setUpdatedBy(playLaunchChannel.getUpdatedBy());

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
        List<PlayLaunchChannel> channels = readerRepository.findAlwaysOnChannelsByNextScheduledTime(true,
                new Date(Long.MIN_VALUE), DateUtils.addMinutes(new Date(), 15));
        channels.forEach(c -> {
            Hibernate.initialize(c.getTenant());
            Hibernate.initialize(c.getPlay());
        });
        return channels;
    }

    private PlayLaunchChannel verifyNewPlayLaunchChannel(PlayLaunchChannel playLaunchChannel) {
        playLaunchChannel.setId(playLaunchChannel.generateChannelId());
        LookupIdMap lookupIdMap = findLookupIdMap(playLaunchChannel);
        if (lookupIdMap != null) {
            playLaunchChannel.setLookupIdMap(lookupIdMap);
        } else {
            throw new NullPointerException("Cannot find lookupIdMap for given lookup id map id");
        }
        verifyChannelConfigHasSameDestinationAsLookupIdMap(lookupIdMap, playLaunchChannel);

        return playLaunchChannel;
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

    private LookupIdMap findLookupIdMap(PlayLaunchChannel playLaunchChannel) {
        if (playLaunchChannel.getLookupIdMap() == null) {
            throw new NullPointerException("No LookupIdMap given for Channel");
        }
        String lookupIdMapId = playLaunchChannel.getLookupIdMap().getId();
        if (lookupIdMapId == null) {
            throw new NullPointerException("Lookup map Id cannot be null.");
        }
        return lookupIdMappingEntityMgr.getLookupIdMap(lookupIdMapId);
    }

}
