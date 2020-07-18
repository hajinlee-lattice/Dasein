package com.latticeengines.apps.dcp.service.impl;

import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;

@Service
public class AppendConfigServiceImpl implements AppendConfigService  {

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace) {
        return primeMetadataProxy.getBlockDrtMatrix();
    }

    @Override
    public DplusAppendConfig getAppendConfig(String customerSpace, String sourceId) {
        //FIXME: to be changed to honor data block entitlement in IDaaS
        List<String> elementIds = Arrays.asList( //
                "duns_number", //
                "primaryname", //
                "countryisoalpha2code", //
                "tradestylenames_name", //
                "primaryaddr_street_line1", //
                "primaryaddr_street_line2", //
                "primaryaddr_addrlocality_name", //
                "primaryaddr_addrregion_name", //
                "primaryaddr_postalcode", //
                "primaryaddr_country_name", //
                "telephone_telephonenumber", //
                "primaryindcode_ussicv4" //
        );
        DplusAppendConfig appendConfig = new DplusAppendConfig();
        appendConfig.setElementIds(elementIds);
        return appendConfig;
    }

}
