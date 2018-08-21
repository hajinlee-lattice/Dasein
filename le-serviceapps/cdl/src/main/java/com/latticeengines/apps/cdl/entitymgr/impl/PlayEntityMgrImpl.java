package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Triple;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PlayDao;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitable;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.repository.PlayRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("playEntityMgr")
public class PlayEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<PlayRepository, Play, Long> //
        implements PlayEntityMgr, GraphVisitable {

    @Inject
    private PlayDao playDao;

    @Inject
    private PlayEntityMgrImpl _self;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Resource(name = "playWriterRepository")
    private PlayRepository playWriterRepository;

    @Resource(name = "playReaderRepository")
    private PlayRepository playReaderRepository;

    @Override
    protected PlayRepository getReaderRepo() {
        return playReaderRepository;
    }

    @Override
    protected PlayRepository getWriterRepo() {
        return playWriterRepository;
    }

    @Override
    protected PlayEntityMgrImpl getSelf() {
        return _self;
    }

    @Override
    public BaseDao<Play> getDao() {
        return playDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Play createPlay(Play play) {
        createNewPlay(play);
        playDao.create(play);
        return play;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Play updatePlay(Play play, Play existingPlay) {
        // Front end only sends delta to the back end to update existing
        updateExistingPlay(existingPlay, play);
        playDao.update(existingPlay);
        return existingPlay;
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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findAllByRatingEnginePid(long pid) {
        return playDao.findAllByRatingEnginePid(pid);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Play> findByRatingEngineAndPlayStatusIn(RatingEngine ratingEngine, List<PlayStatus> statusList) {
        return playWriterRepository.findByRatingEngineAndPlayStatusIn(ratingEngine, statusList);
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
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getAllDeletedPlayIds(boolean forCleanupOnly) {
        return playDao.findAllDeletedPlayIds(forCleanupOnly);
    }

    @Override
    public Set<Triple<String, String, String>> extractDependencies(Play play) {
        String ratingId = play.getRatingEngine().getId();
        RatingEngine rating = findRatingEngine(play);
        String targetSegmentName = rating.getSegment().getName();

        Set<Triple<String, String, String>> attrDepSet = new HashSet<Triple<String, String, String>>();
        attrDepSet.add(ParsedDependencies.tuple(targetSegmentName, //
                VertexType.SEGMENT, EdgeType.DEPENDS_ON_FOR_TARGET));
        attrDepSet.add(ParsedDependencies.tuple(ratingId, //
                VertexType.RATING_ENGINE, EdgeType.DEPENDS_ON));
        attrDepSet.add(ParsedDependencies.tuple(BusinessEntity.Rating + "." + ratingId, //
                VertexType.RATING_ATTRIBUTE, EdgeType.DEPENDS_ON));
        return attrDepSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void accept(GraphVisitor visitor, String entityId) throws Exception {
        Play entity = getPlayByName(entityId, false);
        visitor.visit(entity, parse(entity, null));
    }
}