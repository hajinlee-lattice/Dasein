package com.latticeengines.release.cli;

import java.util.Arrays;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.release.exposed.activities.Activity;
import com.latticeengines.release.exposed.domain.ProcessContext;

public class ReleaseCLI {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("release-context.xml");

        // invokeJMX(applicationContext);
        // invokeHipChat(applicationContext);
        invokeJenkins(applicationContext);
        // invokeJira(applicationContext);
        // invokeNeux(applicationContext);
    }

    public static void invokeJMX(ApplicationContext applicationContext) {

        Activity ac = (Activity) applicationContext.getBean("jmxCheckActivity");
        ac.execute(new ProcessContext());

    }

    public static void invokeHipChat(ApplicationContext applicationContext) {

        Activity ac = (Activity) applicationContext.getBean("finishReleaseNotificationActivity");
        ProcessContext c = ac.execute(new ProcessContext());
        System.out.println(c.getStatusCode());
    }

    public static void invokeJenkins(ApplicationContext applicationContext) {

        ProcessContext context = new ProcessContext();
        context.setUrl("http://bodcdevvldp117.lattice.local:8080/view/DeploymentTests/job/ledp_release_deploymenttests_prodcluster_api");
        context.setReleaseVersion("2.0.7");
        context.setNextReleaseVersion("2.0.8");
        Activity ac = (Activity) applicationContext.getBean("runJenkinsDeploymentTestActivity");
        ProcessContext c = ac.execute(context);
        System.out.print(c.getResponseMessage());
    }

    public static void invokeJira(ApplicationContext applicationContext) {

        ProcessContext context = new ProcessContext();
        context.setProduct("Testing PLS");
        context.setReleaseVersion("1.0.0");
        Activity ac = (Activity) applicationContext.getBean("createChangeManagementJiraActivity");
        ProcessContext c = ac.execute(context);
        System.out.print(c.getStatusCode());
    }

    public static void invokeNeux(ApplicationContext applicationContext) {
        ProcessContext context = new ProcessContext();
        context.setProjectsShouldUploadToNexus(Arrays.asList(new String[] { "le-pls" }));
        context.setReleaseVersion("2.0.7");
        Activity ac = (Activity) applicationContext.getBean("uploadProjectsToNexusActivity");
        ProcessContext c = ac.execute(context);
        System.out.print(c.getStatusCode());
    }
}
