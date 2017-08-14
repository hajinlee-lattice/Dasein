package com.latticeengines.proxy.exposed.eai;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.network.exposed.eai.EaiJobDetailInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("eaiJobDetailProxy")
public class EaiJobDetailProxy extends MicroserviceRestApiProxy implements EaiJobDetailInterface {
    public EaiJobDetailProxy() {
        super("eai");
    }

    @Override
    public EaiImportJobDetail getImportJobDetailByCollectionIdentifier(String collectionIdentifier) {
        String url = constructUrl("/jobdetail/collectionIdentifier/{extractIdentifier}", collectionIdentifier);
        return get("getEaiImportJobDetail", url, null, EaiImportJobDetail.class);
    }

    @Override
    public void cancelImportJob(String collectionIdentifier) {
        String url = constructUrl("/jobdetail/{extractIdentifier}/cancel", collectionIdentifier);
        post("cancelEaiImportJobByIdentifier", url, null, Void.class);
    }

    @Override
    public EaiImportJobDetail getImportJobDetailByAppId(String applicationId) {
        String url = constructUrl("/jobdetail/applicationId/{applicationId}", applicationId);
        return get("getEaiImportJobDetail", url, null, EaiImportJobDetail.class);
    }
}
