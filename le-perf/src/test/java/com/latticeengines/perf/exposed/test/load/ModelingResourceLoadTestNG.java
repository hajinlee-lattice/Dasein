package com.latticeengines.perf.exposed.test.load;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;
import com.latticeengines.perf.job.configuration.ConstructModelConfiguration;
import com.latticeengines.perf.job.runnable.impl.ConstructModel;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;
import com.latticeengines.perf.job.runnable.impl.SubmitModel;
import com.latticeengines.perf.loadframework.PerfLoadTestNGBase;

public class ModelingResourceLoadTestNG extends PerfLoadTestNGBase {

    @Test(groups = "load", enabled = true)
    public void modelingResource() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "on board");

        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
        for (int i = 0; i < numOfRuns; i++) {
            for (int j = 0; j < numOfCustomers; j++) {
                String customer = "c" + j;
                String hdfsPath = customerBaseDir + "/" + customer;

                while (fs.isDirectory(new Path(hdfsPath))) {
                    log.info("delete hdfs path for " + customer);
                    fs.delete(new Path(hdfsPath), true);
                }

                ConstructModelConfiguration cmc = createConstructModelConfiguration(customer);
                ConstructModel cm = new ConstructModel(yarnConfiguration, hdfsPath);
                cm.setConfiguration(restEndpointHost, cmc);
                Future<List<String>> future = executor.submit(cm);
                futures.add(future);
            }
            List<String> appIds = new ArrayList<String>();
            for (Future<List<String>> future : futures) {
                List<String> appId = future.get();
                assertNotNull(appId);
                appIds.addAll(appId);
            }
            assertTrue(GetJobStatus.checkStatus(restEndpointHost, appIds));
        }
    }

    @Test(groups = "load", enabled = true, dependsOnMethods = { "modelingResource" })
    public void preemptionSubmit() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "submit");

        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
        for (int i = 0; i < numOfRuns; i++) {
            int priority = 1;
            while (priority >= 0) {
                modelDef = produceModelDef(priority--);
                for (int j = 0; j < numOfCustomers; j++) {
                    String customer = "c" + j;
                    model = produceAModel(customer);
                    SubmitModel sm = new SubmitModel();
                    sm.setConfiguration(restEndpointHost, model);
                    Future<List<String>> future = executor.submit(sm);
                    futures.add(future);
                }
            }
        }
        List<String> appIds = new ArrayList<String>();
        for (Future<List<String>> future : futures) {
            List<String> appId = future.get();
            assertNotNull(appId);
            appIds.addAll(appId);
        }
        assertTrue(GetJobStatus.checkStatus(restEndpointHost, appIds));
    }
}
