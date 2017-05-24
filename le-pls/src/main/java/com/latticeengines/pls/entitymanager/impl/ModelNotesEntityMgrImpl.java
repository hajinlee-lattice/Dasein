package com.latticeengines.pls.entitymanager.impl;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.ModelNotesOrigin;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.pls.dao.ModelNotesDao;
import com.latticeengines.pls.dao.ModelSummaryDao;
import com.latticeengines.pls.entitymanager.ModelNotesEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;

@Component("modelNotesEntityMgr")
public class ModelNotesEntityMgrImpl extends BaseEntityMgrImpl<ModelNotes> implements ModelNotesEntityMgr{

    @Autowired
    private ModelNotesDao modelNotesDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelNotes> getAllByModelSummaryId(String modelSummaryId) {
        return modelNotesDao.getAllByModelSummaryId(modelSummaryId);
    }

    @Override
    public BaseDao<ModelNotes> getDao() {
        return modelNotesDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public ModelNotes findByNoteId(String noteId) {
        return modelNotesDao.findByNoteId(noteId);
    }

}
