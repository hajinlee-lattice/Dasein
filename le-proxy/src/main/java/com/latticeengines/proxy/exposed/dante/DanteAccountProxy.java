package com.latticeengines.proxy.exposed.dante;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.network.exposed.dante.DanteAccountInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("danteAccountProxy")
public class DanteAccountProxy extends MicroserviceRestApiProxy implements DanteAccountInterface {

    public DanteAccountProxy() {
        super("/dante/accounts");
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<List<DanteAccount>> getAccounts(int count, String customerSpace) {
        String url = constructUrl("/" + count + "?customerSpace=" + customerSpace);
        return get("getAccounts", url, ResponseDocument.class);
    }
}
