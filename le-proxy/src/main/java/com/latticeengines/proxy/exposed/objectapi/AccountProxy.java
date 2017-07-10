package com.latticeengines.proxy.exposed.objectapi;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.network.exposed.objectapi.AccountInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("accountProxy")
public class AccountProxy extends MicroserviceRestApiProxy implements AccountInterface {
    public AccountProxy() {
        super("objectapi/customerspaces");
    }

    @Override
    public DataPage getAccounts(String customerSpace, String start, Integer offset, Integer pageSize,
            Boolean hasSfdcAccountId, DataRequest dataRequest) {
        String url = constructUrl(
                "/{customerSpace}/accounts/data?start={start}&offset={offset}&pageSize={pagesize}&hasSfdcAccountId={hasSfdcAccountId}",
                customerSpace, start, offset, pageSize, hasSfdcAccountId);
        return post("get Data", url, dataRequest, DataPage.class);
    }
}
