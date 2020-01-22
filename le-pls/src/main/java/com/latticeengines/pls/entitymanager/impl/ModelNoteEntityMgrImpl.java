package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.pls.dao.ModelNoteDao;
import com.latticeengines.pls.entitymanager.ModelNoteEntityMgr;

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
    public ModelNote findByNoteId(String noteId) {
        return modelNotesDao.findByNoteId(noteId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void deleteById(String id) {
        modelNotesDao.deleteById(id);
    }

}
