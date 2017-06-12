package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.pls.dao.PlayDao;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;

@Component("playEntityMgr")
public class PlayEntityMgrImpl extends BaseEntityMgrImpl<Play> implements PlayEntityMgr {

    private static final Log log = LogFactory.getLog(PlayEntityMgrImpl.class);

    @Autowired
    private PlayDao playDao;

    @Override
    public BaseDao<Play> getDao() {
        return playDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Play createOrUpdatePlay(Play play) {
        Date timestamp = new Date(System.currentTimeMillis());
        if (play.getName() == null) {
            play.setTimeStamp(timestamp);
            play.setLastUpdatedTimestamp(timestamp);
            play.setName(play.generateNameStr());
            playDao.create(play);
            return play;
        } else {
            Play retrievedPlay = findByName(play.getName());
            if (retrievedPlay == null) {
                log.warn(String.format("Play with name %s does not exist, creating it now", play.getName()));
                play.setTimeStamp(timestamp);
                play.setLastUpdatedTimestamp(timestamp);
                playDao.create(play);
                return play;
            } else {
                // Front end only sends delta to the back end to update existing
                updateExistingPlay(retrievedPlay, play);
                retrievedPlay.setLastUpdatedTimestamp(timestamp);
                playDao.update(retrievedPlay);
                return retrievedPlay;
            }
        }

    }

    private void updateExistingPlay(Play existingPlay, Play play) {
        if (play.getDisplayName() != null) {
            existingPlay.setDisplayName(play.getDisplayName());
        }
        if (play.getDescription() != null) {
            existingPlay.setDescription(play.getDescription());
        }
        if (play.getCallPrep() != null) {
            existingPlay.setCallPrep(play.getCallPrep());
        }
        if (play.getSegmentName() != null) {
            existingPlay.setSegment(play.getSegment());
        }
        if (play.getSegment() != null) {
            existingPlay.setSegment(play.getSegment());
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
        if (play.getName() == null) {
            throw new NullPointerException("Play name cannot be null.");
        }

        Play existing = findByName(play.getName());
        if (existing != null) {
            delete(existing);
        }
        create(play);
    }

}
