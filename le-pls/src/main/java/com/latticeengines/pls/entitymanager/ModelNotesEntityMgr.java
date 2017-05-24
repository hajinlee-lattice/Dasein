package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.NoteParams;

public interface ModelNotesEntityMgr extends BaseEntityMgr<ModelNotes> {

    List<ModelNotes> getAllByModelSummaryId(String modelSummaryId);

    ModelNotes findByNoteId(String noteId);

}
