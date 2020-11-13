package com.latticeengines.query.evaluator.athena;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

@Component
public class AthenaQueryTester {

    @Inject
    protected QueryEvaluatorService queryEvaluatorService;

    @Inject
    protected Configuration yarnConfiguration;

    protected AttributeRepository attrRepo;
    protected CustomerSpace customerSpace;

    public AttributeRepository getAttrRepo() {
        return attrRepo;
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setupTestContext(CustomerSpace customerSpace, AttributeRepository attrRepo) {
        this.customerSpace = customerSpace;
        this.attrRepo = attrRepo;
    }

    public long getCountFromAthena(Query query) {
        long count1 = queryEvaluatorService.getCount(attrRepo, query, ATHENA_USER);
        // test idempotent
        long count2 = queryEvaluatorService.getCount(attrRepo, query, ATHENA_USER);
        Assert.assertEquals(count1, count2);
        return count1;
    }

    public DataPage getDataFromAthena(Query query) {
        return queryEvaluatorService.getData(attrRepo, query, ATHENA_USER);
    }

}
