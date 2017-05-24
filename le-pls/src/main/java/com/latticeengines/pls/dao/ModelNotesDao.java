package com.latticeengines.pls.dao;


import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.NoteParams;

public interface ModelNotesDao extends BaseDao<ModelNotes>{

    List<ModelNotes> getAllByModelSummaryId(String modelSummaryId);

    ModelNotes findByNoteId(String noteId);
}
