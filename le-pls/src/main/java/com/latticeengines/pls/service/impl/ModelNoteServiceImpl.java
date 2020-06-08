package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.pls.service.ModelNoteService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;

@Component("modelNoteService")
public class ModelNoteServiceImpl implements ModelNoteService {

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Override
    public void create(String modelSummaryId, NoteParams noteParams) {
        modelSummaryProxy.create(modelSummaryId, noteParams);
    }

    @Override
    public void deleteById(String id) {
        modelSummaryProxy.deleteById(id);
    }

    @Override
    public void updateById(String id, NoteParams noteParams) {
        modelSummaryProxy.updateById(id, noteParams);
    }

    @Override
    public List<ModelNote> getAllByModelSummaryId(String modelSummaryId) {
        return modelSummaryProxy.getAllByModelSummaryId(MultiTenantContext.getTenant().getId(),
                modelSummaryId, false, false, true);
    }

    @Override
    public void copyNotes(String sourceModelSummaryId, String targetModelSummaryId) {
        modelSummaryProxy.copyNotes(sourceModelSummaryId, targetModelSummaryId);
    }
}
