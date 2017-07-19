package com.latticeengines.proxy.exposed.objectapi;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.EntityLookup;
import com.latticeengines.domain.exposed.query.PageFilter;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.util.QueryTranslator;
import com.latticeengines.domain.exposed.util.ReverseQueryTranslator;
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
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        String url = constructUrl("/{customerSpace}/accounts/count?start={start}", customerSpace, start);
        return post("get Count", url, dataRequest, Integer.class);
    }

    @Override
    public DataPage getAccounts(String customerSpace, String start, Integer offset, Integer pageSize,
            DataRequest dataRequest) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        String url = constructUrl("/{customerSpace}/accounts/data?start={start}&offset={offset}&pageSize={pagesize}",
                customerSpace, start, offset, pageSize);
        return post("get Data", url, dataRequest, DataPage.class);
    }

    public long getAccountsCount(String customerSpace, Restriction restriction) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setFrontEndRestriction(ReverseQueryTranslator.translateRestriction(restriction));
        Query query = QueryTranslator.translate(frontEndQuery, null);
        query.addLookup(new EntityLookup(BusinessEntity.Account));
        return entityProxy.getCount(customerSpace, query);
    }

    public DataPage getAccounts(String customerSpace, Restriction restriction, Integer offset, Integer pageSize,
            DataRequest dataRequest) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        PageFilter pageFilter = new PageFilter(offset, pageSize);
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setFrontEndRestriction(ReverseQueryTranslator.translateRestriction(restriction));
        frontEndQuery.setPageFilter(pageFilter);
        Query query = QueryTranslator.translate(frontEndQuery, null);
        return entityProxy.getData(customerSpace, query);
    }
}
