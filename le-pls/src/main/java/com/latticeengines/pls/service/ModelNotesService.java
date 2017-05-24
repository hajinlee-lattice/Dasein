package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.NoteParams;

public interface ModelNotesService {
    void create(String modelSummaryId, NoteParams noteParams);

    List<ModelNotes> getAllByModelSummaryId(String modelSummaryId);

    void deleteByNoteId(String noteId);

    void updateByNoteId(String noteId, NoteParams noteParams);

    void copyNotes(String fromModelSummaryId, String toModelSummaryId);
}
