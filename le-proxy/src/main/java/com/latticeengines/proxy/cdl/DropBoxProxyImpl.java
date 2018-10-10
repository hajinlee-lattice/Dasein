package com.latticeengines.proxy.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessRequest;
import com.latticeengines.domain.exposed.cdl.GrantDropBoxAccessResponse;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

@Component("dropBoxProxy")
public class DropBoxProxyImpl extends MicroserviceRestApiProxy implements DropBoxProxy {

    protected DropBoxProxyImpl() {
        super("cdl");
    }

    @Override
    public DropBoxSummary getDropBox(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/", //
                shortenCustomerSpace(customerSpace));
        return get("get drop box", url, DropBoxSummary.class);
    }

    @Override
    public GrantDropBoxAccessResponse grantAccess(String customerSpace, GrantDropBoxAccessRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/access", //
                shortenCustomerSpace(customerSpace));
        return put("grant drop box access", url, request, GrantDropBoxAccessResponse.class);
    }

    @Override
    public GrantDropBoxAccessResponse refreshAccessKey(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/dropbox/key", //
                shortenCustomerSpace(customerSpace));
        return put("refresh dropbox access key", url, null, GrantDropBoxAccessResponse.class);
    }

}
