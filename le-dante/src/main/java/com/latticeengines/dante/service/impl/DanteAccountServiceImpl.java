package com.latticeengines.dante.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.entitymgr.DanteAccountEntityMgr;
import com.latticeengines.dante.service.DanteAccountService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
@Component("danteAccountService")
public class DanteAccountServiceImpl implements DanteAccountService {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(DanteAccountServiceImpl.class);

    @Autowired
    private DanteAccountEntityMgr danteAccountEntityMgr;

    public List<DanteAccount> getAccounts(int count, String customerSpace) {
        if (count < 1) {
            throw new LedpException(LedpCode.LEDP_38004);
        }

        List<DanteAccount> accounts = danteAccountEntityMgr.getAccounts(count, getCustomerID(customerSpace));

        if (accounts == null || accounts.size() < 1) {
            throw new LedpException(LedpCode.LEDP_38003, new String[] { customerSpace });
        } else
            return accounts;
    }

    private String getCustomerID(String customerSpaceStr) {
        try {
            CustomerSpace customerSpace = CustomerSpace.parse(customerSpaceStr);
            return customerSpace.getTenantId();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_38009, e, new String[] { customerSpaceStr });
        }
    }
}
