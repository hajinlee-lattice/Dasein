package com.latticeengines.serviceflows.workflow.modeling;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ModelStepConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.NoteOrigin;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.serviceflows.workflow.core.InternalResourceRestApiProxy;

@Component("createNote")
public class CreateNote extends BaseModelStep<ModelStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(CreateNote.class);

    private InternalResourceRestApiProxy proxy = null;
    @Override
    public void execute() {
        if (proxy == null) {
            proxy = new InternalResourceRestApiProxy(configuration.getInternalResourceHostPort());
        }
        ModelSummary sourceModelSummary = configuration.getSourceModelSummary();

        @SuppressWarnings("unchecked")
        Map<String, String> eventToModelId = getObjectFromContext(EVENT_TO_MODELID, Map.class);
        Set<String> modelIds = new HashSet<String>(eventToModelId.values());
        String content = configuration.getNotesContent();
        if(content != null && !content.trim().equals("")) {
            String userName = configuration.getUserName();
            NoteParams noteParams = new NoteParams();
            noteParams.setContent(content);
            noteParams.setUserName(userName);
            if(sourceModelSummary != null) {
                noteParams.setOrigin(NoteOrigin.REMODEL.getOrigin());
            }
            else {
                noteParams.setOrigin(NoteOrigin.MODELCREATED.getOrigin());
            }

            log.info(String.format("Create a new note by user %s", userName));
            for(String modelId : modelIds) {
                proxy.createNote(modelId, noteParams);
            }
        }
        if(sourceModelSummary != null) {
            log.info(String.format("Copy all notes according to ModelSummaryId %s", sourceModelSummary.getId()));
            for(String modelId : modelIds) {
                proxy.copyNotes(sourceModelSummary.getId(), modelId);
            }

        }
    }
}
