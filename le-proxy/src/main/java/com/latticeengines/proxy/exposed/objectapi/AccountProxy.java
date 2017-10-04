package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.network.exposed.objectapi.AccountInterface;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("accountProxy")
public class AccountProxy extends MicroserviceRestApiProxy implements AccountInterface {

    @Autowired
    private EntityProxy entityProxy;

    public AccountProxy() {
        super("objectapi/customerspaces");
    }

    @Override
    public long getAccountsCount(String customerSpace, String start, DataRequest dataRequest) {
        String url = constructUrl("/{customerSpace}/accounts/count?start={start}", shortenCustomerSpace(customerSpace),
                start);
        return post("get Count", url, dataRequest, Integer.class);
    }

    @Override
    public DataPage getAccounts(String customerSpace, String start, Long offset, Long pageSize,
            DataRequest dataRequest) {
        String url = constructUrl("/{customerSpace}/accounts/data?start={start}&offset={offset}&pageSize={pagesize}",
                shortenCustomerSpace(customerSpace), start, offset, pageSize);
        return post("get Data", url, dataRequest, DataPage.class);
    }

    public DataPage getAccounts(String customerSpace, Restriction restriction, Long offset, Long pageSize,
            List<String> fields) {
        return getAccounts(customerSpace, restriction, offset, pageSize, fields, null);
    }

    public DataPage getAccounts(String customerSpace, Restriction restriction, Long offset, Long pageSize,
            List<String> fields, String sortField) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        PageFilter pageFilter = new PageFilter(offset, pageSize);
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setAccountRestriction(new FrontEndRestriction(restriction));
        frontEndQuery.setPageFilter(pageFilter);
        frontEndQuery.addLookups(BusinessEntity.Account, fields.toArray(new String[fields.size()]));
        frontEndQuery.setMainEntity(BusinessEntity.Account);
        return entityProxy.getData(customerSpace, frontEndQuery);
    }

}
