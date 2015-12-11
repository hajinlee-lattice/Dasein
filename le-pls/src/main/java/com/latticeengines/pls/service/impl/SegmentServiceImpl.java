package com.latticeengines.pls.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Segment;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.liaison.exposed.service.ConnectionMgr;
import com.latticeengines.liaison.exposed.service.ConnectionMgrFactory;
import com.latticeengines.liaison.exposed.service.Query;
import com.latticeengines.liaison.exposed.service.QueryColumn;
import com.latticeengines.pls.entitymanager.SegmentEntityMgr;
import com.latticeengines.pls.service.SegmentService;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.exposed.util.SecurityUtils;

@Component("segmentService")
public class SegmentServiceImpl implements SegmentService {

    private static final String visiDBManagerType = "visiDB";
    private static final String modelingQueryName = "Q_PLS_Modeling";
    private static final String scoringBulkQueryName = "Q_PLS_Scoring_Bulk";
    private static final String scoringIncrQueryName = "Q_PLS_Scoring_Incremental";
    private static final String eventFcnName = "P1_Event";
    private static final String modelGuidFcnName = "Model_GUID";

    @Autowired
    private SessionService sessionService;

    @Autowired
    private SegmentEntityMgr segmentEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ConnectionMgrFactory connectionMgrFactory;

    @Autowired
    private TenantConfigService tenantConfigService;

    @Override
    public void createSegment(Segment segment, HttpServletRequest request) {
        Tenant tenant = SecurityUtils.getTenantFromRequest(request, sessionService);
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        segment.setTenant(tenant);
        segmentEntityMgr.create(segment);
    }

    @Override
    public void update(String segmentName, Segment segment) {
        Segment segmentFromDb = segmentEntityMgr.findByName(segmentName);

        if (segmentFromDb == null) {
            throw new LedpException(LedpCode.LEDP_18025, new String[] { segmentName });
        }

        if (!segmentName.equals(segment.getName())) {
            throw new LedpException(LedpCode.LEDP_18026);
        }

        segmentFromDb.setModelId(segment.getModelId());
        segmentFromDb.setPriority(segment.getPriority());
        segmentEntityMgr.update(segmentFromDb);
    }

    @Override
    public void synchronizeModelingAndScoring(Tenant tenant) {
        try {
            String tenantName = CustomerSpace.parse(tenant.getId()).getTenantId();
            String dlUrl = tenantConfigService.getDLRestServiceAddress(tenant.getId());
            ConnectionMgr connMgr = connectionMgrFactory.getConnectionMgr(visiDBManagerType, tenantName, dlUrl);

            Query modelingQuery = connMgr.getQuery(modelingQueryName);
            Query scoringIncrQuery = connMgr.getQuery(scoringIncrQueryName);
            QueryColumn modelGuidCol = scoringIncrQuery.getColumn(modelGuidFcnName);
            QueryColumn eventNullCol = scoringIncrQuery.getColumn(eventFcnName);

            List<QueryColumn> scoringCols = new ArrayList<>();
            List<QueryColumn> modelingCols = modelingQuery.getColumns();
            for (QueryColumn modelingCol : modelingCols) {
                if (!eventFcnName.equals(modelingCol.getName())){
                    scoringCols.add(modelingCol);
                } else {
                    scoringCols.add(modelGuidCol);
                    scoringCols.add(eventNullCol);
                }
            }

            boolean updateRequired = false;
            List<String> scoringOriginalCols = scoringIncrQuery.getColumnNames();
            for (QueryColumn scoringCol : scoringCols) {
                if(!scoringOriginalCols.contains(scoringCol.getName())) {
                    updateRequired = true;
                    break;
                }
            }

            if (updateRequired) {
                Query scoringBulkQuery = connMgr.getQuery(scoringBulkQueryName);
                scoringBulkQuery.setColumns(scoringCols);
                scoringIncrQuery.setColumns(scoringCols);
                connMgr.setQuery(scoringBulkQuery);
                connMgr.setQuery(scoringIncrQuery);
            }
        } catch (IOException ex) {
            throw new LedpException(LedpCode.LEDP_18073, ex, new String[] { ex.getMessage() });
        }
    }
}
