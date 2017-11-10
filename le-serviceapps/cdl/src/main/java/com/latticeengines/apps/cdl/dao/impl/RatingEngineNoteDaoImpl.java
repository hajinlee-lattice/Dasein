package com.latticeengines.apps.cdl.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.RatingEngineNoteDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;

@Component("ratingEngineNoteDao")
public class RatingEngineNoteDaoImpl extends BaseDaoImpl<RatingEngineNote> implements RatingEngineNoteDao {

    @Override
    protected Class<RatingEngineNote> getEntityClass() {
        return RatingEngineNote.class;
    }

}
