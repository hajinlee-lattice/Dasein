package com.latticeengines.release.cli;

import java.util.Arrays;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.latticeengines.release.exposed.activities.Activity;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.processes.ReleaseProcess;

public class ReleaseCLI {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("release-context.xml");
        ProcessContext context = new ProcessContext();
        context.setReleaseVersion(args[0]);
        context.setNextReleaseVersion(args[1]);
        context.setProduct(args[2]);
        context.setRevision(args[3]);
        context.setProjectsShouldUploadToNexus(Arrays.asList(new String[] { "le-pls", "le-propdata" }));
        invokeProcess(applicationContext, context);

    }

    private static void invokeProcess(ApplicationContext applicationContext, ProcessContext context) {
        ReleaseProcess rp = (ReleaseProcess) applicationContext.getBean("releaseDPProcess");
        rp.execute(context);
    }

    public static void invokeJenkins(ApplicationContext applicationContext) {
        ProcessContext context = new ProcessContext();
        context.setReleaseVersion("2.0.7");
        context.setNextReleaseVersion("2.0.8");
        Activity ac = (Activity) applicationContext.getBean("dpDeploymentTestActivity");
        ProcessContext c = ac.execute(context);
        System.out.print(c.getResponseMessage());
    }
}
