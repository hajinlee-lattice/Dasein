package com.latticeengines.pls.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.ModelNote;
import com.latticeengines.pls.dao.ModelNoteDao;

@Component("modelNotesDao")
public class ModelNoteDaoImpl extends BaseDaoImpl<ModelNote> implements ModelNoteDao {
    @SuppressWarnings("unchecked")
    @Override
    public List<ModelNote> getAllByModelSummaryId(String modelSummaryId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelNote> entityClz = getEntityClass();
        String queryStr = String.format("from %s where MODEL_ID = :modelSummaryId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("modelSummaryId", modelSummaryId);
        return query.list();
    }

    @Override
    protected Class<ModelNote> getEntityClass() {
        return ModelNote.class;
    }

    @Override
    public ModelNote findByNoteId(String noteId) {
        Session session = getSessionFactory().getCurrentSession();
        Class<ModelNote> entityClz = getEntityClass();
        String queryStr = String.format("from %s where ID = :noteId", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("noteId", noteId);
        return (ModelNote) query.list().get(0);
    }

    @Override
    public void deleteById(String id) {
        Session session = getSessionFactory().getCurrentSession();
        Query query = session.createQuery("delete from " + getEntityClass().getSimpleName() + " where id = :id")
                .setParameter("id", id);
        query.executeUpdate();
    }

}
