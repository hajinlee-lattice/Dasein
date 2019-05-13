package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PlayDao;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitable;
import com.latticeengines.apps.cdl.entitymgr.GraphVisitor;
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayGroupEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.SegmentEntityMgr;
import com.latticeengines.apps.cdl.repository.PlayRepository;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.graph.EdgeType;
import com.latticeengines.domain.exposed.graph.ParsedDependencies;
import com.latticeengines.domain.exposed.graph.VertexType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.pls.PlayStatus;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("playEntityMgr")
public class PlayEntityMgrImpl extends BaseReadWriteRepoEntityMgrImpl<PlayRepository, Play, Long> //
        implements PlayEntityMgr, GraphVisitable {

    private static final Logger log = LoggerFactory.getLogger(PlayEntityMgrImpl.class);

    @Inject
    private PlayDao playDao;

    @Inject
    private PlayEntityMgrImpl _self;

    @Inject
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    @Inject
    private SegmentEntityMgr segmentEntityMgr;

    @Inject
    private PlayGroupEntityMgr playGroupEntityMgr;

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

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Long countByPlayTypePid(Long pid) {
        return playReaderRepository.countByPlayType_Pid(pid);
    }

    private void createNewPlay(Play play) {
        if (play.getRatingEngine() != null) {
            play.setRatingEngine(findRatingEngine(play));
        }
        if (play.getPlayGroups() != null) {
            play.setPlayGroups(findPlayGroups(play));

        }
        if (play.getTargetSegment() == null) {
            throw new LedpException(LedpCode.LEDP_18206);
        } else {
            String segmentName = play.getTargetSegment().getName();
            if (StringUtils.isBlank(segmentName)) {
                throw new LedpException(LedpCode.LEDP_18207);
            }
            MetadataSegment selSegment = segmentEntityMgr.findByName(segmentName.trim());
            play.setTargetSegment(selSegment);
        }
        // TODO: Remove in M24
        if (play.getDisplayName() == null) {
            play.setDisplayName(String.format(Play.DEFAULT_NAME_PATTERN, Play.DATE_FORMAT.format(new Date())));
        }
        if (play.getName() == null) {
            play.setName(play.generateNameStr());
        }
    }

    private RatingEngine findRatingEngine(Play play) {
        if (play.getRatingEngine() == null) {
            throw new NullPointerException("No Model associated with play.");
        }
        String ratingEngineId = play.getRatingEngine().getId();
        if (ratingEngineId == null) {
            throw new NullPointerException("Rating Engine Id cannot be null.");
        }
        return ratingEngineEntityMgr.findById(ratingEngineId);
    }

    private MetadataSegment findTargetSegment(Play play) {
        String segmentName = play.getTargetSegment().getName();
        if (segmentName == null) {
            throw new NullPointerException("Segment Name cannot be null.");
        }
        return segmentEntityMgr.findByName(segmentName);
    }

    private Set<PlayGroup> findPlayGroups(Play plays) {
        if (plays.getPlayGroups() == null) {
            throw new NullPointerException("No Play Group in Play");
        }
        Set<PlayGroup> playGroups = new HashSet<PlayGroup>();
        for (PlayGroup playGroup : plays.getPlayGroups()) {
            String playGroupId = playGroup.getId();
            if (playGroupId == null) {
                throw new NullPointerException("Play Group Id cannot be null.");
            }
            PlayGroup existingPlayGroup = playGroupEntityMgr.findById(playGroupId);
            if (existingPlayGroup == null) {
                throw new NullPointerException("No PlayGroup found for given ID");
            }
            playGroups.add(existingPlayGroup);
        }
        return playGroups;
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
        if (play.getPlayGroups() != null) {
            existingPlay.setPlayGroups(findPlayGroups(play));
        }
        if (play.getPlayType() != null && !play.getPlayType().getId().equals(existingPlay.getPlayType().getId())) {
            existingPlay.setPlayType(play.getPlayType());
        }
        existingPlay.setUpdatedBy(play.getUpdatedBy());
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
        Set<Triple<String, String, String>> attrDepSet = null;
        if (play != null) {
            attrDepSet = new HashSet<>();
            String targetSegmentName = findTargetSegment(play).getName();
            attrDepSet.add(ParsedDependencies.tuple(targetSegmentName, //
                    VertexType.SEGMENT, EdgeType.DEPENDS_ON));

            if (play.getRatingEngine() != null) {
                String ratingId = play.getRatingEngine().getId();
                RatingEngine rating = findRatingEngine(play);
                String ratingEngineSegment = rating.getSegment().getName();
                attrDepSet.add(ParsedDependencies.tuple(ratingEngineSegment, //
                        VertexType.SEGMENT, EdgeType.DEPENDS_ON));
                attrDepSet.add(ParsedDependencies.tuple(ratingId, //
                        VertexType.RATING_ENGINE, EdgeType.DEPENDS_ON));
                attrDepSet.add(ParsedDependencies.tuple(BusinessEntity.Rating + "." + ratingId, //
                        VertexType.RATING_ATTRIBUTE, EdgeType.DEPENDS_ON));
            }
        }
        if (CollectionUtils.isNotEmpty(attrDepSet)) {
            log.info(String.format("Extracted dependencies from play %s: %s", play.getName(),
                    JsonUtils.serialize(attrDepSet)));
        }
        return attrDepSet;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public void accept(GraphVisitor visitor, Object entity) throws Exception {
        visitor.visit((Play) entity, parse((Play) entity, null));
    }
}
