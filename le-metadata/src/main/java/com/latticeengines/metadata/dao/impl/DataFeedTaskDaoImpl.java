package com.latticeengines.metadata.dao.impl;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.metadata.dao.DataFeedTaskDao;

@Component("datafeedTaskDao")
public class DataFeedTaskDaoImpl extends BaseDaoImpl<DataFeedTask> implements DataFeedTaskDao {

    @Override
    protected Class<DataFeedTask> getEntityClass() {
        return DataFeedTask.class;
    }

    @Override
    public void createDataFeedTask(DataFeedTask dataFeedTask) {
        super.create(dataFeedTask);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public DataFeedTask getDataFeedTask(String source, String dataFeedType, String entity, Long dataFeed) {
        Session session = sessionFactory.getCurrentSession();
        Class<DataFeedTask> entityClz = getEntityClass();
        String queryStr = String.format("from %s where feedType = :feedType and entity = :entity and " +
                "source = :source and dataFeed = :dataFeed", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("feedType", dataFeedType);
        query.setString("entity", entity);
        query.setString("source", source);
        query.setLong("dataFeed", dataFeed);
        List list = query.list();
        //List<DataFeedTask> result = new ArrayList<>();
        if (list.size() == 0) {
            return null;
        }
        return (DataFeedTask)list.get(0);
    }
}
