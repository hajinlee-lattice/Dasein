package com.latticeengines.query.evaluator.presto;

import static com.latticeengines.query.factory.PrestoQueryProvider.PRESTO_USER;

import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.prestodb.exposed.service.PrestoDbService;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

@Component
public class PrestoQueryTester {

    @Inject
    protected QueryEvaluatorService queryEvaluatorService;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private PrestoDbService prestoDbService;

    protected AttributeRepository attrRepo;
    protected Map<String, String> tblPathMap;
    protected CustomerSpace customerSpace;

    public AttributeRepository getAttrRepo() {
        return attrRepo;
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setupTestContext(CustomerSpace customerSpace, AttributeRepository attrRepo,
            Map<String, String> tblPathMap) {
        this.customerSpace = customerSpace;
        this.attrRepo = attrRepo;
        this.tblPathMap = tblPathMap;

        tblPathMap.forEach((tblName, path) -> {
            prestoDbService.deleteTableIfExists(tblName);
            if (path.endsWith(".parquet")) {
                prestoDbService.createTableIfNotExists(tblName, path, DataUnit.DataFormat.PARQUET, null);
            } else {
                prestoDbService.createTableIfNotExists(tblName, path, DataUnit.DataFormat.AVRO);
            }
        });

    }

    public long getCountFromPresto(Query query) {
        long count1 = queryEvaluatorService.getCount(attrRepo, query, PRESTO_USER);
        // test idempotent
        long count2 = queryEvaluatorService.getCount(attrRepo, query, PRESTO_USER);
        Assert.assertEquals(count1, count2);
        return count1;
    }

    public DataPage getDataFromPresto(Query query) {
        return queryEvaluatorService.getData(attrRepo, query, PRESTO_USER);
    }

}
