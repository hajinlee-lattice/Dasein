package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.pls.dao.PlayDao;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.entitymanager.RatingEngineEntityMgr;

@Component("playEntityMgr")
public class PlayEntityMgrImpl extends BaseEntityMgrImpl<Play> implements PlayEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(PlayEntityMgrImpl.class);

    @Autowired
    private PlayDao playDao;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

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
            Play retrievedPlay = findByName(play.getName());
            if (retrievedPlay == null) {
                log.warn(String.format("Play with name %s does not exist, creating it now", play.getName()));
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
        play.setName(play.generateNameStr());
        play.setDisplayName(String.format(Play.DEFAULT_NAME_PATTERN, Play.DATE_FORMAT.format(new Date())));
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
        if (play.getExcludeItemsWithoutSalesforceId() != null) {
            existingPlay.setExcludeItemsWithoutSalesforceId(play.getExcludeItemsWithoutSalesforceId());
        }
        if (play.getRatingEngine() != null) {
            play.setRatingEngine(findRatingEngine(play));
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
    public Play findByName(String name) {
        return playDao.findByName(name);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteByName(String name) {
        Play play = findByName(name);
        if (play == null) {
            throw new NullPointerException(String.format("Play with name %s cannot be found", name));
        }
        super.delete(play);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(Play play) {
        createOrUpdatePlay(play);
    }

}
