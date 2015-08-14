package com.latticeengines.release.cli;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.processes.ReleaseProcess;

public class ReleaseCLI {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("release-context.xml");
        ProcessContext processContext = (ProcessContext) applicationContext.getBean("processContext");;
        processContext.setReleaseVersion(args[0]);
        processContext.setNextReleaseVersion(args[1]);
        processContext.setProduct(args[2]);
        processContext.setRevision(args[3]);
        invokeProcess(applicationContext, processContext);

    }

    public static ReleaseProcess getReleaseProcessInstance(ApplicationContext applicationContext, ProcessContext processContext){
        if(processContext.getProduct().equalsIgnoreCase("modeling platform")){
            return  (ReleaseProcess) applicationContext.getBean("releaseDPProcess");
        }else if(processContext.getProduct().equalsIgnoreCase("lp")){
            return (ReleaseProcess) applicationContext.getBean("releasePLSProcess");
        }else if(processContext.getProduct().equalsIgnoreCase("all")){
            return (ReleaseProcess) applicationContext.getBean("releaseAllProductsProcess");
        }
        return null;
    }

    private static void invokeProcess(ApplicationContext applicationContext, ProcessContext processContext) {
        ReleaseProcess rp = getReleaseProcessInstance (applicationContext, processContext);
        rp.execute();
    }
}
