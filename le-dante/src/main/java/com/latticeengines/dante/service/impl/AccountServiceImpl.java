package com.latticeengines.dante.service.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.entitymgr.AccountEntityMgr;
import com.latticeengines.dante.service.AccountService;
import com.latticeengines.domain.exposed.dante.DanteAccount;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("accountService")
public class AccountServiceImpl implements AccountService {
    private static final Logger log = Logger.getLogger(AccountServiceImpl.class);

    @Autowired
    private AccountEntityMgr accountEntityMgr;

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
}
