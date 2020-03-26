package com.latticeengines.pls.service.impl.dcp;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dcp.Source;
import com.latticeengines.domain.exposed.dcp.SourceRequest;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.dcp.SourceService;
import com.latticeengines.proxy.exposed.dcp.SourceProxy;

@Service("dcpSourceService")
public class SourceServiceImpl implements SourceService {

    @Inject
    private SourceProxy sourceProxy;

    @Override
    public Source createSource(SourceRequest sourceRequest) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.createSource(customerSpace.toString(), sourceRequest);
    }

    @Override
    public Source getSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.getSource(customerSpace.toString(), sourceId);
    }

    @Override
    public List<Source> getSourceList(String projectId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.getSourceList(customerSpace.toString(), projectId);
    }

    @Override
    public Boolean deleteSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.deleteSource(customerSpace.toString(), sourceId);
    }

    @Override
    public Boolean pauseSource(String sourceId) {
        CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        if (customerSpace == null) {
            throw new LedpException(LedpCode.LEDP_18217);
        }
        return sourceProxy.pauseSource(customerSpace.toString(), sourceId);
    }
}
