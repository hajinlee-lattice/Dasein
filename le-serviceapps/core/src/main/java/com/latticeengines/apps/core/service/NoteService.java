package com.latticeengines.apps.core.service;

import com.latticeengines.domain.exposed.pls.NoteParams;

public interface NoteService<Note> {

    Note findById(String id);

    void deleteById(String id);

    void updateById(String id, NoteParams noteParams);
}
