package com.latticeengines.pls.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DantePreviewResources;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;
import com.latticeengines.domain.exposed.cdl.TalkingPointNotionAttributes;
import com.latticeengines.domain.exposed.cdl.TalkingPointPreview;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.service.TalkingPointService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.TalkingPointProxy;
import com.latticeengines.proxy.exposed.cdl.TalkingPointsAttributesProxy;

@Component("talkingPointService")
public class TalkingPointServiceImpl implements TalkingPointService {

    @Inject
    private PlayProxy playProxy;

    @Inject
    private TalkingPointsAttributesProxy talkingPointsAttributesProxy;

    @Inject
    private TalkingPointProxy talkingPointProxy;

    @Override
    public List<TalkingPointDTO> createOrUpdate(List<TalkingPointDTO> talkingPoints) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointProxy.createOrUpdate(tenant.getId(), talkingPoints);
    }

    @Override
    public TalkingPointDTO findByName(String talkingPointName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return talkingPointProxy.findByName(tenant.getId(), talkingPointName);
    }

    @Override
    public List<TalkingPointDTO> findAllByPlayName(String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        return talkingPointProxy.findAllByPlayName(tenant.getId(), playName);
    }

    @Override
    public DantePreviewResources getPreviewResources() {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointProxy.getPreviewResources(tenant.getId());
    }

    @Override
    public TalkingPointPreview preview(String playName) {
        try {
            Tenant tenant = MultiTenantContext.getTenant();
            if (tenant == null) {
                throw new LedpException(LedpCode.LEDP_38008);
            }
            return talkingPointProxy.getTalkingPointPreview(tenant.getId(), playName);
        } catch (LedpException e) {
            throw e;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38015, e, new String[]{playName, null});
        }
    }

    @Override
    public void publish(String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        playProxy.publishTalkingPoints(tenant.getId(), playName);
    }

    @Override
    public List<TalkingPointDTO> revert(String playName) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointProxy.revert(tenant.getId(), playName);
    }

    @Override
    public void delete(@PathVariable String talkingPointName) {
        Tenant tenant = MultiTenantContext.getTenant();
        talkingPointProxy.deleteByName(tenant.getId(), talkingPointName);
    }

    @Override
    public TalkingPointNotionAttributes getAttributesByNotions(List<String> notions) {
        Tenant tenant = MultiTenantContext.getTenant();
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_38008);
        }
        return talkingPointsAttributesProxy.getAttributesByNotions(tenant.getId(), notions);
    }

}
