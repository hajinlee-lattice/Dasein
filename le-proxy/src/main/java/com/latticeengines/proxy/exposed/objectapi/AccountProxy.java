package com.latticeengines.proxy.exposed.objectapi;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.network.exposed.objectapi.AccountInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("accountProxy")
public class AccountProxy extends MicroserviceRestApiProxy implements AccountInterface {
    public AccountProxy() {
        super("objectapi/customerspaces");
    }

    @Override
    public long getAccountsCount(String customerSpace, String start, DataRequest dataRequest) {
        String url = constructUrl("/{customerSpace}/accounts/count?start={start}", customerSpace, start);
        return post("get Count", url, dataRequest, Integer.class);
    }

    @Override
    public DataPage getAccounts(String customerSpace, String start, Integer offset, Integer pageSize,
            DataRequest dataRequest) {
        String url = constructUrl("/{customerSpace}/accounts/data?start={start}&offset={offset}&pageSize={pagesize}",
                customerSpace, start, offset, pageSize);
        return post("get Data", url, dataRequest, DataPage.class);
    }

    public int getSegmentAccountsCount(String customerSpace, Restriction segmentRestrictionQuery) {
        return 0;
    }

    public DataPage getSegmentAccounts(String customerSpace, Restriction segmentRestrictionQuery, Integer offset,
            Integer pageSize, DataRequest dataRequest) {
        return null;
    }
}
