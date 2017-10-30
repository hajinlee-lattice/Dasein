package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.pls.dao.RatingEngineNoteDao;
import com.latticeengines.pls.entitymanager.RatingEngineNoteEntityMgr;

@Component("RatingEngineNoteEntityMgr")
public class RatingEngineNoteEntityMgrImpl extends BaseEntityMgrImpl<RatingEngineNote>
        implements RatingEngineNoteEntityMgr {

    @Autowired
    private RatingEngineNoteDao ratingEngineNoteDao;

    @Override
    public BaseDao<RatingEngineNote> getDao() {
        return ratingEngineNoteDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<RatingEngineNote> getAllByRatingEngine(RatingEngine ratingEngine) {
        if (ratingEngine == null || ratingEngine.getPid() == null) {
            throw new NullPointerException(
                    String.format("Rating Engine with id of %s cannot be found", ratingEngine.getId()));
        }
        return ratingEngineNoteDao.findAllByField("FK_RATING_ENGINE_ID", ratingEngine.getPid());
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public RatingEngineNote findById(String id) {
        return ratingEngineNoteDao.findByField("ID", id);
    }

}
