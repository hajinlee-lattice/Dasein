package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;
import com.latticeengines.pls.dao.RatingEngineNoteDao;

@Component("ratingEngineNoteDao")
public class RatingEngineNoteDaoImpl extends BaseDaoImpl<RatingEngineNote> implements RatingEngineNoteDao {

    @Override
    protected Class<RatingEngineNote> getEntityClass() {
        return RatingEngineNote.class;
    }

}
