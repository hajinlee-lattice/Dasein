package com.latticeengines.propdata.api.datasource;

import java.util.Map;
import java.util.concurrent.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.propdata.api.testframework.PropDataApiFunctionalTestNGBase;

public class MatcherContextHolderTestNG extends PropDataApiFunctionalTestNGBase {

    @Autowired
    private PropDataMatcherRoutingDataSource dataSource;

    private ExecutorService executor = Executors.newFixedThreadPool(MatcherClient.values().length);

    @Test(groups = "api.functional")
    public void switchMatcherClient() throws InterruptedException, ExecutionException {
        Map<MatcherClient, Future<Boolean>> results = new ConcurrentHashMap<>();
        for (MatcherClient client: MatcherClient.values()) {
            results.put(client, executor.submit(verifyMatcherClientCallable(client)));
        }
        for (Map.Entry<MatcherClient, Future<Boolean>> result: results.entrySet()) {
            Assert.assertTrue(result.getValue().get(), "MatcherClient test failed on " + result.getKey().name());
        }
    }


    private Callable<Boolean> verifyMatcherClientCallable(final MatcherClient client) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                MatcherContextHolder.setMatcherClient(client);
                String url = dataSource.getConnection().getMetaData().getURL();
                return url.contains(client.getHost());
            }
        };
    }

}
