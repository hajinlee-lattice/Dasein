package com.latticeengines.proxy.dcp;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.dcp.AppendConfigProxy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component("appendConfigProxy")
public class AppendConfigProxyImpl extends MicroserviceRestApiProxy implements AppendConfigProxy {

    protected AppendConfigProxyImpl() {
        super("dcp");
    }

    private DataBlockEntitlementContainer filterDataBlockContainer(
            DataBlockEntitlementContainer container,
            String domainName,
            String recordType) {
        if (domainName.isEmpty() && recordType.isEmpty()) {
            return container;
        } else {
            List<DataBlockEntitlementContainer.Domain> resultDomains = new ArrayList();

            for (DataBlockEntitlementContainer.Domain domain: container.getDomains()) {
                if(!domainName.isEmpty() && domain.getDomain().getDisplayName() != domainName) {
                    continue;
                }

                if (recordType.isEmpty()) {
                    resultDomains.add(domain);
                } else {
                    Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> filteredRecords =
                            domain.getRecordTypes()
                                    .entrySet()
                                    .stream()
                                    .filter(entry -> entry.getKey().getDisplayName() == recordType)
                                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()));

                    resultDomains.add(new DataBlockEntitlementContainer.Domain(domain.getDomain(), filteredRecords));
                }
            }

            return new DataBlockEntitlementContainer(resultDomains);
        }
    }

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace, String domainName, String recordType) {
        String url = constructUrl("/customerspaces/{customerSpace}/append-config/entitlement", //
                shortenCustomerSpace(customerSpace));
        return filterDataBlockContainer(
                get("get entitlement", url, DataBlockEntitlementContainer.class),
                domainName,
                recordType);
    }

}
