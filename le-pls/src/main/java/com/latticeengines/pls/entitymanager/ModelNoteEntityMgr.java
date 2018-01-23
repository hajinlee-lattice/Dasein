package com.latticeengines.pls.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelNote;

public interface ModelNoteEntityMgr extends BaseEntityMgr<ModelNote> {

    List<ModelNote> getAllByModelSummaryId(String modelSummaryId);

    ModelNote findByNoteId(String noteId);

    void deleteById(String id);

}
