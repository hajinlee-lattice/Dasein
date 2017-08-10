package com.latticeengines.proxy.exposed.objectapi;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;
import java.util.stream.Collectors;

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

    public long getAccountsCount(String customerSpace, Restriction restriction) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setFrontEndRestriction(new FrontEndRestriction(restriction));
        return entityProxy.getCount(customerSpace, BusinessEntity.Account, frontEndQuery);
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
        frontEndQuery.setFrontEndRestriction(new FrontEndRestriction(restriction));
        frontEndQuery.setPageFilter(pageFilter);
        frontEndQuery.addLookups(BusinessEntity.Account, fields.toArray(new String[fields.size()]));
        return entityProxy.getData(customerSpace, BusinessEntity.Account, frontEndQuery);
    }

    /*
     * TODO - need to be implemented by Bernard
     * 
     * This API accepts list of ordered restrictions and list of accountIds. It
     * needs to evaluate which account matches with one of these restrictions
     * (give priority to lowest index matching bucket). This API needs to return
     * list of indexes of matching restrictions for each of the accountIds
     */
    public List<Integer> calculateMatchingRestrictionIdx(String customerSpace, List<Restriction> orderedRestrictions,
            List<Long> accountIds) {
        List<Integer> accountToMatchingRestrictionIndexList = //
                accountIds.stream().//
                        map(accountId -> {
                            return 0;
                        }).collect(Collectors.toList());
        return accountToMatchingRestrictionIndexList;
    }
}
