package com.latticeengines.perf.exposed.test.load;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.latticeengines.perf.job.configuration.ConstructModelConfiguration;
import com.latticeengines.perf.job.runnable.impl.ConstructModel;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;
import com.latticeengines.perf.loadframework.PerfLoadTestNGBase;

public class ModelingResourceLoadTestNG extends PerfLoadTestNGBase {

    @Test(groups = "load", enabled = true)
    public void onBoard() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "on board");

        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
        for (int i = 0; i < numOfRuns; i++) {
            for (int j = 0; j < numOfCustomers; j++) {
                String customer = "c" + j;
                fs.delete(new Path(customerBaseDir + "/" + customer), true);

                ConstructModelConfiguration cmc = createConstructModelConfiguration(customer);
                ConstructModel cm = new ConstructModel();
                cm.setConfiguration(restEndpointHost, cmc);
                Future<List<String>> future = executor.submit(cm);
                futures.add(future);
            }
            List<String> appIds = new ArrayList<String>();
            for (Future<List<String>> future : futures) {
                appIds.addAll(future.get());
            }
            GetJobStatus.checkStatus(restEndpointHost, appIds);
        }
    }
}
