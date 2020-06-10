package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.ModelNoteDao;
import com.latticeengines.apps.lp.entitymgr.ModelNoteEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelNote;

@Component("modelNotesEntityMgr")
public class ModelNoteEntityMgrImpl extends BaseEntityMgrImpl<ModelNote> implements ModelNoteEntityMgr {

    @Inject
    private ModelNoteDao modelNotesDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelNote> getAllByModelSummaryId(String modelSummaryId) {
        return modelNotesDao.getAllByModelSummaryId(modelSummaryId);
    }

    @Override
    public BaseDao<ModelNote> getDao() {
        return modelNotesDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelNote getByNoteId(String noteId) {
        return modelNotesDao.findByNoteId(noteId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void removeById(String id) {
        modelNotesDao.deleteById(id);
    }

}
