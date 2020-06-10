package com.latticeengines.pls.service;

import com.latticeengines.domain.exposed.pls.NoteParams;

public interface NoteService<Note> {

    void deleteById(String id);

    void updateById(String id, NoteParams noteParams);
}
