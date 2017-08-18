package com.latticeengines.dante.service.impl;

import java.util.Date;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.entitymgr.DanteLeadEntityMgr;
import com.latticeengines.dante.service.DanteLeadService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DanteLead;
import com.latticeengines.domain.exposed.dante.DanteLeadNotionObject;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;

@Component("danteLeadService")
public class DanteLeadServiceImpl implements DanteLeadService {
    private static final Logger log = LoggerFactory.getLogger(DanteLeadServiceImpl.class);

    @Autowired
    private DanteLeadEntityMgr danteLeadEntityMgr;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    void setInternalResourceRestApiProxy(InternalResourceRestApiProxy internalResourceRestApiProxy) {
        this.internalResourceRestApiProxy = internalResourceRestApiProxy;
    }

    @PostConstruct
    public void initialize() throws Exception {
        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Override
    public void create(Recommendation recommendation, String customerSpace) {
        if (recommendation == null) {
            throw new LedpException(LedpCode.LEDP_38021, new String[]{customerSpace});
        }
        Play play;
        try {
            play = internalResourceRestApiProxy.findPlayByName(CustomerSpace.parse(customerSpace),
                    recommendation.getPlayId());
            if (play == null) {
                throw new LedpException(LedpCode.LEDP_38012, new String[]{recommendation.getPlayId()});
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38013, e);
        }

        try {
            danteLeadEntityMgr.create(convertForDante(recommendation, customerSpace, play));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38020, e, new String[]{recommendation.getId(), customerSpace});
        }
    }

    private DanteLead convertForDante(Recommendation recommendation, String customerSpace, Play play) {
        DanteLead lead = new DanteLead();
        Date now = new Date();
        lead.setAccountExternalID(recommendation.getLeAccountExternalID());
        lead.setCreationDate(now);
        lead.setCustomerID(CustomerSpace.parse(customerSpace).getTenantId());
        lead.setExternalID(recommendation.getId());
        lead.setLastModificationDate(now);
        lead.setSalesforceID(recommendation.getSfdcAccountID());
        lead.setValue(JsonUtils.serialize(new DanteLeadNotionObject(recommendation, play)));
        return lead;
    }
}
