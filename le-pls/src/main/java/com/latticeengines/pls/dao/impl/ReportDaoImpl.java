package com.latticeengines.pls.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Report;
import com.latticeengines.pls.dao.ReportDao;

@Component("reportDao")
public class ReportDaoImpl extends BaseDaoImpl<Report> implements ReportDao {

    @Override
    protected Class<Report> getEntityClass() {
        return Report.class;
    }
}
