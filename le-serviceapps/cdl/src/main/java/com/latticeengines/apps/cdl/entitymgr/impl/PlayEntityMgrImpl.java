package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PlayDao;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.repository.writer.PlayRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;

@Component("playEntityMgr")
public class PlayEntityMgrImpl extends BaseEntityMgrRepositoryImpl<Play, Long> implements PlayEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(PlayEntityMgrImpl.class);

    @Inject
    private PlayDao playDao;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private PlayRepository playRepository;

    @Override
    public BaseJpaRepository<Play, Long> getRepository() {
        return playRepository;
    }

    @Override
    public BaseDao<Play> getDao() {
        return playDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Play createOrUpdatePlay(Play play) {
        if (play.getName() == null) {
            createNewPlay(play);
            playDao.create(play);
            return play;
        } else {
            Play retrievedPlay = getPlayByName(play.getName(), true);
            if (retrievedPlay == null) {
                log.warn(String.format("Play with name %s does not exist, creating it now", play.getName()));
                createNewPlay(play);
                playDao.create(play);
                return play;
            } else {
                // Front end only sends delta to the back end to update existing
                updateExistingPlay(retrievedPlay, play);
                playDao.update(retrievedPlay);
                return retrievedPlay;
            }
        }
    }

    private void createNewPlay(Play play) {
        if (play.getRatingEngine() != null) {
            play.setRatingEngine(findRatingEngine(play));
        }
        if (play.getDisplayName() == null) {
            play.setDisplayName(String.format(Play.DEFAULT_NAME_PATTERN, Play.DATE_FORMAT.format(new Date())));
        }
        if (play.getName() == null) {
            play.setName(play.generateNameStr());
        }
    }

    private RatingEngine findRatingEngine(Play play) {
        String ratingEngineId = play.getRatingEngine().getId();
        if (ratingEngineId == null) {
            throw new NullPointerException("Rating Engine Id cannot be null.");
        }
        return ratingEngineEntityMgr.findById(ratingEngineId);
    }

    private void updateExistingPlay(Play existingPlay, Play play) {
        if (play.getDisplayName() != null) {
            existingPlay.setDisplayName(play.getDisplayName());
        }
        if (play.getDescription() != null) {
            existingPlay.setDescription(play.getDescription());
        }
        if (play.getPlayStatus() != null) {
            existingPlay.setPlayStatus(play.getPlayStatus());
        }
        if (play.getRatingEngine() != null) {
            existingPlay.setRatingEngine(findRatingEngine(play));
        }
        if (play.getDeleted() != null) {
            existingPlay.setDeleted(play.getDeleted());
        }
        if (play.getIsCleanupDone() != null) {
            existingPlay.setIsCleanupDone(play.getIsCleanupDone());
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void delete(Play entity) {
        playDao.delete(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findAllByRatingEnginePid(long pid) {
        return playDao.findAllByRatingEnginePid(pid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findByRatingEngineAndPlayStatusIn(RatingEngine ratingEngine, List<PlayStatus> statusList) {
        return playRepository.findByRatingEngineAndPlayStatusIn(ratingEngine, statusList);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Play getPlayByName(String name, Boolean considerDeleted) {
        return playDao.findByName(name, considerDeleted == Boolean.TRUE);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByName(String name, Boolean hardDelete) {
        Play play = getPlayByName(name, true);
        if (play == null) {
            throw new NullPointerException(String.format("Play with name %s cannot be found", name));
        }
        playDao.deleteByPid(play.getPid(), hardDelete == Boolean.TRUE);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(Play play) {
        createOrUpdatePlay(play);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getAllDeletedPlayIds(boolean forCleanupOnly) {
        return playDao.findAllDeletedPlayIds(forCleanupOnly);
    }
}
