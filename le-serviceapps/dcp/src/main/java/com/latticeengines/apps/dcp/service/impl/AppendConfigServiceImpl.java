package com.latticeengines.apps.dcp.service.impl;

import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_BASE_INFO;
import static com.latticeengines.domain.exposed.datacloud.manage.DataBlock.BLOCK_ENTITY_RESOLUTION;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.apps.dcp.service.AppendConfigService;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockEntitlementContainer;
import com.latticeengines.domain.exposed.datacloud.manage.DataBlockLevel;
import com.latticeengines.domain.exposed.datacloud.manage.DataDomain;
import com.latticeengines.domain.exposed.datacloud.manage.DataRecordType;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;
import com.latticeengines.proxy.exposed.matchapi.PrimeMetadataProxy;

@Service
public class AppendConfigServiceImpl implements AppendConfigService  {

    @Inject
    private PrimeMetadataProxy primeMetadataProxy;

    @Override
    public DataBlockEntitlementContainer getEntitlement(String customerSpace) {
        //FIXME: to be changed to honor data block entitlement in IDaaS
        return getDefaultEntitlement();
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

    // by default, a tenant is entitled to (entityresolution, baseinfo, companyinfo_l1)
    private DataBlockEntitlementContainer getDefaultEntitlement() {
        DataBlockEntitlementContainer container = primeMetadataProxy.getBlockDrtMatrix();
        Set<String> entitledBlocks = ImmutableSet.<String>builder() //
                .add(BLOCK_BASE_INFO) //
                .add(BLOCK_ENTITY_RESOLUTION) //
                .add("companyinfo") //
                .build();
        List<DataBlockEntitlementContainer.Domain> domains = container.getDomains().stream() //
                .filter(domain -> DataDomain.SalesMarketing.equals(domain.getDomain())) //
                .map(domain -> {
                    Map<DataRecordType, List<DataBlockEntitlementContainer.Block>> types = //
                            new HashMap<>(domain.getRecordTypes());
                    for (DataRecordType recordType: types.keySet()) {
                        List<DataBlockEntitlementContainer.Block> blocks = types.get(recordType).stream() //
                                .filter(block -> entitledBlocks.contains(block.getBlockId())) //
                                .map(block -> {
                                    if ("companyinfo".equals(block.getBlockId())) {
                                        return new DataBlockEntitlementContainer.Block(block.getBlockId(), //
                                                Collections.singletonList(DataBlockLevel.L1));
                                    } else {
                                        return block;
                                    }
                                })
                                .collect(Collectors.toList());
                        types.put(recordType, blocks);
                    }
                    return new DataBlockEntitlementContainer.Domain(domain.getDomain(), types);
                })
                .collect(Collectors.toList());
        return new DataBlockEntitlementContainer(domains);
    }

}
