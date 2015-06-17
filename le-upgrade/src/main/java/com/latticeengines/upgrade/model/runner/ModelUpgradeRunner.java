package com.latticeengines.upgrade.model.runner;

import java.io.IOException;

import javax.crypto.NoSuchPaddingException;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.latticeengines.upgrade.model.service.ModelUpgradeService;

public class ModelUpgradeRunner {

    private ModelUpgradeService modelUgprade;

    public ModelUpgradeRunner(){
        @SuppressWarnings("resource")
        ApplicationContext ac = new ClassPathXmlApplicationContext("upgrade-context.xml");
        this.modelUgprade = (ModelUpgradeService) ac.getBean("model_140_Upgrade");
    }

    public static void main(String[] args) throws IOException, Exception, NoSuchPaddingException {
        ModelUpgradeRunner runner = new ModelUpgradeRunner();
        runner.run();
    }

    private void run() throws Exception {
        modelUgprade.upgrade();
    }

}
