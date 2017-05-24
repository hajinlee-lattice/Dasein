package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelNotes;
import com.latticeengines.domain.exposed.pls.NoteParams;
import com.latticeengines.pls.dao.ModelNotesDao;

@Component("modelNotesDao")
public class ModelNotesDaoImpl extends BaseDaoImpl<ModelNotes> implements ModelNotesDao {
    @SuppressWarnings("unchecked")
    @Override
    public List<ModelNotes> getAllByModelSummaryId(String modelSummaryId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelNotes> entityClz = getEntityClass();
        String queryStr = String
                .format("from %s where MODEL_ID = :modelSummaryId",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelSummaryId", modelSummaryId);
        return query.list();
    }

    @Override
    protected Class<ModelNotes> getEntityClass() {
        return ModelNotes.class;
    }

    @Override
    public ModelNotes findByNoteId(String noteId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelNotes> entityClz = getEntityClass();
        String queryStr = String
                .format("from %s where ID = :noteId",
                        entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("noteId", noteId);
        return (ModelNotes) query.list().get(0);
    }

}
