package com.latticeengines.eai.commandline;

import org.apache.camel.main.Main;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class EaiMain extends Main {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(EaiMain.class);
    
    public EaiMain() {
        try (ConfigurableApplicationContext ctx = new ClassPathXmlApplicationContext("eai-context.xml")) {}
    }
    
    public static void main(String[] args) throws Exception {
        EaiMain main = new EaiMain();
        main.enableHangupSupport();
        main.run(args);
    }
}
