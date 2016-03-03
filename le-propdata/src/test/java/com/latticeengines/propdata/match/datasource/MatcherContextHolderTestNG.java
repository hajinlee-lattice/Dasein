package com.latticeengines.propdata.match.datasource;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.MatchClient;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

public class MatcherContextHolderTestNG extends PropDataMatchFunctionalTestNGBase {

    @Autowired
    private MatchClientRoutingDataSource dataSource;

    private ExecutorService executor = Executors.newFixedThreadPool(MatchClient.values().length);

    @Test(groups = "api", enabled = false)
    public void switchMatcherClient() throws InterruptedException, ExecutionException {
        Map<MatchClient, Future<Boolean>> results = new ConcurrentHashMap<>();
        for (MatchClient client: MatchClient.values()) {
            results.put(client, executor.submit(verifyMatcherClientCallable(client)));
        }
        for (Map.Entry<MatchClient, Future<Boolean>> result: results.entrySet()) {
            Assert.assertTrue(result.getValue().get(), "MatcherClient test failed on " + result.getKey().name());
        }
    }


    private Callable<Boolean> verifyMatcherClientCallable(final MatchClient client) {
        return new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                MatchClientContextHolder.setMatchClient(client);
                String url = dataSource.getConnection().getMetaData().getURL();
                MatchClientDocument doc = new MatchClientDocument(client);
                return url.contains(doc.getHost());
            }
        };
    }

}
