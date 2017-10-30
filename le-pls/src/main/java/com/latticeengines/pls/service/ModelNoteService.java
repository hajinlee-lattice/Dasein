package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.NoteParams;

public interface ModelNoteService extends NoteService<ModelNote> {

    void create(String modelSummaryId, NoteParams noteParams);

    List<ModelNote> getAllByModelSummaryId(String modelSummaryId);

    void copyNotes(String sourceModelSummaryId, String targetModelSummaryId);

}
