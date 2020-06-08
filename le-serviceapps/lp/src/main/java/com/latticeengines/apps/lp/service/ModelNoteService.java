package com.latticeengines.apps.lp.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.NoteParams;

public interface ModelNoteService {

    void deleteById(String id);

    void updateById(String id, NoteParams noteParams);

    void create(String modelSummaryId, NoteParams noteParams);

    List<ModelNote> getAllByModelSummaryId(String modelId, boolean returnRelational, boolean returnDocument, boolean validOnly);

    void copyNotes(String sourceModelSummaryId, String targetModelSummaryId);

}
