package com.latticeengines.dante.service.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dante.entitymgr.AccountEntityMgr;
import com.latticeengines.dante.service.AccountService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("accountService")

public class AccountServiceImpl implements AccountService {
    @Autowired
    private AccountEntityMgr accountEntityMgr;

    private final Path metadataDocumentPath = new Path("/MetadataDocument.json");

    public List<DanteAccount> getAccounts(int count) {
        if (count < 1) {
            throw new LedpException(LedpCode.LEDP_38004);
        }

        List<DanteAccount> accounts = accountEntityMgr.getAccounts(count);

        if (accounts == null || accounts.size() < 1) {
            throw new LedpException(LedpCode.LEDP_38003, new String[] { "TODO: Add tenant Name here" }); // TODO:JLM
        } else
            return accounts;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> getAccountAttributes() {
        // Camille camille = CamilleEnvironment.getCamille();
        // CustomerSpace customerSpace = MultiTenantContext.getCustomerSpace();
        // Path docPath =
        // PathBuilder.buildCustomerSpacePath(CamilleEnvironment.getPodId(),
        // customerSpace.getContractId(),
        // customerSpace.getTenantId(), customerSpace.getSpaceId()) //
        // .append(PathConstants.SERVICES) //
        // .append(PathConstants.DANTE) //
        // .append(metadataDocumentPath);

        return JsonUtils.deserialize(tempStr, Map.class);
    }

    private String tempStr = "{\"Company\" : [{\"value\":\"Account.Address1\",\"name\":\"Address1\"},{\"value\":\"Account.Address2\",\"name\":\"Address2\"},{\"value\":\"Account.City\",\"name\":\"City\"},{\"value\":\"Account.Country\",\"name\":\"Country\"},{\"value\":\"Account.DisplayName\",\"name\":\"Company Name\"},{\"value\":\"Account.EstimatedRevenue\",\"name\":\"Estimated Revenue\"},{\"value\":\"Account.LastModified\",\"name\":\"Last Modification Date\"},{\"value\":\"Account.NAICSCode\",\"name\":\"NAICS Code\"},{\"value\":\"Account.OwnerDisplayName\",\"name\":\"Sales Rep\"},{\"value\":\"Account.SICCode\",\"name\":\"SIC Code\"},{\"value\":\"Account.StateProvince\",\"name\":\"StateProvince\"},{\"value\":\"Account.Territory\",\"name\":\"Territory\"},{\"value\":\"Account.Vertical\",\"name\":\"Industry\"},{\"value\":\"Account.Zip\",\"name\":\"Zip\"}]}";
}
