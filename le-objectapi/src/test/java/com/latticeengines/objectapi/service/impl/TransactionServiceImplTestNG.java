package com.latticeengines.objectapi.service.impl;


import java.time.LocalDate;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.objectapi.service.TransactionService;

public class TransactionServiceImplTestNG extends QueryServiceImplTestNGBase {

    @Inject
    private TransactionService transactionService;

    @BeforeClass(groups = "functional")
    public void setup() {
        super.setupTestData(3);
    }

    @Test(groups = "functional")
    public void testMaxTransactionDate() {
        String maxTxnDateStr = transactionService.getMaxTransactionDate(DataCollection.Version.Blue);
        Assert.assertNotNull(maxTxnDateStr);
        LocalDate date = LocalDate.parse(maxTxnDateStr);
        Assert.assertNotNull(date);
        System.out.println(date);
    }


}
