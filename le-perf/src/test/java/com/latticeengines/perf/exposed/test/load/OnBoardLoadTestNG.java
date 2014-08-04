package com.latticeengines.perf.exposed.test.load;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.hadoop.fs.Path;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.Test;
import com.latticeengines.perf.job.runnable.impl.GetJobStatus;
import com.latticeengines.perf.job.runnable.impl.OnBoard;
import com.latticeengines.perf.job.runnable.impl.SubmitModel;
import com.latticeengines.perf.loadframework.PerfLoadTestNGBase;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
public class OnBoardLoadTestNG extends PerfLoadTestNGBase {

    @Test(groups = "load", enabled = true)
    public void onBoard() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + " on board");
        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
        for (int i = 0; i < numOfCustomers; i++) {
            String customer = "c" + i;
            String hdfsPath = customerBaseDir + "/" + customer;

            while (fs.isDirectory(new Path(hdfsPath))) {
                log.info("delete hdfs path for " + customer);
                fs.delete(new Path(hdfsPath), true);
            }

            OnBoard ob = new OnBoard(yarnConfiguration, hdfsPath);
            ob.setConfiguration(restEndpointHost, createOnBoardConfiguration(customer));
            Future<List<String>> future = executor.submit(ob);
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

    @Test(groups = "load", enabled = true, dependsOnMethods = { "onBoard" })
    public void noPreemptionSubmit() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + " submit no preemption");

        List<Future<List<String>>> futures = new ArrayList<Future<List<String>>>();
        for (int i = 0; i < numOfRuns; i++) {
            for (int j = 0; j < numOfCustomers; j++) {
                String customer = "c" + j;
                model = produceAModel(customer);
                SubmitModel sm = new SubmitModel();
                sm.setConfiguration(restEndpointHost, model);
                Future<List<String>> future = executor.submit(sm);
                futures.add(future);
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
