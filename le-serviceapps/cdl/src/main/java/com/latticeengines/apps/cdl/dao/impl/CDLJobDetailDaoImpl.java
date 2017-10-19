package com.latticeengines.apps.cdl.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.dao.CDLJobDetailDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobDetail;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobStatus;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLJobType;

@Component("cdlJobDetailDao")
public class CDLJobDetailDaoImpl extends BaseDaoImpl<CDLJobDetail> implements CDLJobDetailDao {

    @Override
    protected Class<CDLJobDetail> getEntityClass() {
        return CDLJobDetail.class;
    }

    @Override
    public List<CDLJobDetail> listAllRunningJobByJobType(CDLJobType cdlJobType) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(CDLJobDetail.class);
        criteria.add(Restrictions.eq("cdlJobType", cdlJobType));
        criteria.add(Restrictions.eq("cdlJobStatus", CDLJobStatus.RUNNING));
        criteria.addOrder(Order.desc("pid"));
        List list = criteria.list();
        List<CDLJobDetail> details = new ArrayList<>();
        if (list == null | list.size() == 0) {
            return details;
        } else {
            for (int i = 0; i < list.size(); i++) {
                details.add((CDLJobDetail) list.get(i));
            }
            return details;
        }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public CDLJobDetail findLatestJobByJobType(CDLJobType cdlJobType) {
        Session session = sessionFactory.getCurrentSession();
        Criteria criteria = session.createCriteria(CDLJobDetail.class);
        criteria.add(Restrictions.eq("cdlJobType", cdlJobType));
        criteria.addOrder(Order.desc("pid"));
        criteria.setFirstResult(0);
        criteria.setMaxResults(1);
        List list = criteria.list();
        if (list == null || list.size() == 0) {
            return null;
        } else {
            return (CDLJobDetail) list.get(0);
        }
    }
}
