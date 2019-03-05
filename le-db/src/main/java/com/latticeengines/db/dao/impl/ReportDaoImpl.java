package com.latticeengines.db.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.ReportDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.workflow.Report;

@Component("reportDao")
public class ReportDaoImpl extends BaseDaoImpl<Report> implements ReportDao {

    @Override
    protected Class<Report> getEntityClass() {
        return Report.class;
    }
}
