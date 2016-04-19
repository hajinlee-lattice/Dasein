package com.latticeengines.propdata.core.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.propdata.core.testframework.PropDataCoreFunctionalTestNGBase;

public class HdfsPodContextTestNG extends PropDataCoreFunctionalTestNGBase {

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Test(groups = "functional")
    public void testThreadSafeContext() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            Future<Boolean> future = executor.submit(new HdfsPodCallable());
            futures.add(future);
        }

        for (Future<Boolean> future: futures) {
            Assert.assertTrue(future.get());
        }
    }

    private class HdfsPodCallable implements Callable<Boolean> {

        @Override
        public Boolean call() {
            Boolean passed = true;
            for (int i = 0; i < 3; i++) {
                String uuid = UUID.randomUUID().toString().replace("-", "");
                HdfsPodContext.changeHdfsPodId(uuid);
                Assert.assertEquals(hdfsPathBuilder.getHdfsPodId(), uuid);
                passed = passed && uuid.equals(hdfsPathBuilder.getHdfsPodId());
            }
            return passed;
        }

    }

}
