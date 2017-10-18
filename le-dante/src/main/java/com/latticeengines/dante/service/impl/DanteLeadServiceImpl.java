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
import com.latticeengines.domain.exposed.dante.DanteLeadDTO;
import com.latticeengines.domain.exposed.dante.DanteLeadNotionObject;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
    public void create(DanteLeadDTO danteLeadDTO, String customerSpace) {
        if (danteLeadDTO.getRecommendation() == null) {
            throw new LedpException(LedpCode.LEDP_38021, new String[] { customerSpace });
        }

        if (danteLeadDTO.getPlayLaunch() == null) {
            throw new LedpException(LedpCode.LEDP_38022, new String[] { customerSpace });
        }

        try {
            danteLeadEntityMgr.create(convertForDante(danteLeadDTO, customerSpace));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38020, e, new String[] { danteLeadDTO.getRecommendation().getId(),
                    danteLeadDTO.getPlay().getName(), customerSpace });
        }
    }

    private DanteLead convertForDante(DanteLeadDTO danteLeadDTO, String customerSpace) {
        DanteLead lead = new DanteLead();
        Date now = new Date();
        if (org.apache.commons.lang.StringUtils.isBlank(danteLeadDTO.getRecommendation().getSfdcAccountID())) {
            lead.setAccountExternalID(danteLeadDTO.getRecommendation().getLeAccountExternalID());
        } else {
            lead.setAccountExternalID(danteLeadDTO.getRecommendation().getSfdcAccountID());
        }
        lead.setCreationDate(now);
        lead.setCustomerID(CustomerSpace.parse(customerSpace).getTenantId());
        lead.setExternalID(danteLeadDTO.getRecommendation().getId());
        lead.setLastModificationDate(now);
        lead.setSalesforceID(null);
        /// TODO: Remove this once both BIS & DanteUI support String IDs
        lead.setRecommendationID(danteLeadDTO.getRecommendation().getPid().intValue());
        /// TODO: END
        lead.setValue(JsonUtils.serialize(new DanteLeadNotionObject(danteLeadDTO.getRecommendation(),
                danteLeadDTO.getPlay(), danteLeadDTO.getPlayLaunch())));
        return lead;
    }
}
