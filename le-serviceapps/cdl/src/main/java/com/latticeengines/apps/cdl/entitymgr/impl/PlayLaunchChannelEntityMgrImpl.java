package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

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
        createNewPlayLaunchChannel(playLaunchChannel);
        playLaunchChannelDao.create(playLaunchChannel);
        return playLaunchChannel;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public PlayLaunchChannel updatePlayLaunchChannel(PlayLaunchChannel playLaunchChannel,
            PlayLaunchChannel existingPlayLaunchChannel) {
        existingPlayLaunchChannel.setPlayLaunch(
                createOrUpdatePlayLaunch(playLaunchChannel.getPlayLaunch(), existingPlayLaunchChannel.getPlayLaunch()));
        if (playLaunchChannel.getIsAlwaysOn() != null) {
            existingPlayLaunchChannel.setIsAlwaysOn(playLaunchChannel.getIsAlwaysOn());
        }
        playLaunchChannelDao.update(existingPlayLaunchChannel);
        return existingPlayLaunchChannel;
    }

    private PlayLaunch createOrUpdatePlayLaunch(PlayLaunch playLaunch, PlayLaunch existingPlayLaunch) {
        if (existingPlayLaunch.getLaunchState() == LaunchState.UnLaunched) {
            existingPlayLaunch = updatePlayLaunch(playLaunch, existingPlayLaunch);
            playLaunchEntityMgr.update(existingPlayLaunch);
            playLaunch = existingPlayLaunch;
        } else {
            playLaunch.setLaunchId(PlayLaunch.generateLaunchId());
            playLaunch.setLaunchState(LaunchState.UnLaunched);
            playLaunchEntityMgr.create(playLaunch);
        }
        return playLaunch;
    }

    private PlayLaunch updatePlayLaunch(PlayLaunch playLaunch, PlayLaunch existingPlayLaunch) {
        if (playLaunch.getExcludeItemsWithoutSalesforceId() != null) {
            existingPlayLaunch.setExcludeItemsWithoutSalesforceId(playLaunch.getExcludeItemsWithoutSalesforceId());
        }
        if (playLaunch.getTopNCount() != null) {
            existingPlayLaunch.setTopNCount(playLaunch.getTopNCount());
        }
        if (playLaunch.getBucketsToLaunch() != null) {
            existingPlayLaunch.setBucketsToLaunch(playLaunch.getBucketsToLaunch());
        }
        if (playLaunch.isLaunchUnscored()) {
            existingPlayLaunch.setLaunchUnscored(playLaunch.isLaunchUnscored());
        }
        if (playLaunch.getLaunchState() != null) {
            existingPlayLaunch.setLaunchState(playLaunch.getLaunchState());
        }
        return existingPlayLaunch;
    }

    private PlayLaunchChannel createNewPlayLaunchChannel(PlayLaunchChannel playLaunchChannel) {
        playLaunchChannel.setId(playLaunchChannel.generateChannelId());
        LookupIdMap lookupIdMap = findLookupIdMap(playLaunchChannel);
        if (lookupIdMap != null) {
            playLaunchChannel.setLookupIdMap(lookupIdMap);
        } else {
            throw new NullPointerException("Cannot find lookupIdMap for given lookup id map id");
        }
        PlayLaunch playLaunch = playLaunchChannel.getPlayLaunch();
        if (!playLaunch.getDestinationOrgId().equals(lookupIdMap.getOrgId())
                || !playLaunch.getDestinationSysType().equals(lookupIdMap.getExternalSystemType())) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Play launch destination org id and system type is not the same as the System Org given for current channel" });
        }
        return playLaunchChannel;
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
