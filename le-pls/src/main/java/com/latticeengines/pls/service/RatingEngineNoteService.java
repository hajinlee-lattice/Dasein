package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.domain.exposed.pls.RatingEngineNote;

public interface RatingEngineNoteService extends NoteService<RatingEngineNote> {

    RatingEngineNote create(String ratingEngineId, NoteParams noteParams);

    List<RatingEngineNote> getAllByRatingEngineId(String ratingEngineId);

}
