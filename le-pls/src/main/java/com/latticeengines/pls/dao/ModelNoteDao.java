package com.latticeengines.pls.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ModelNote;

public interface ModelNoteDao extends BaseDao<ModelNote> {

    List<ModelNote> getAllByModelSummaryId(String modelSummaryId);

    ModelNote findByNoteId(String noteId);

    void deleteById(String id);
}
