package com.latticeengines.apps.lp.entitymgr;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.ModelNote;

public interface ModelNoteEntityMgr extends BaseEntityMgr<ModelNote> {

    List<ModelNote> getAllByModelSummaryId(String modelSummaryId);

    ModelNote getByNoteId(String noteId);

    void removeById(String id);

}
