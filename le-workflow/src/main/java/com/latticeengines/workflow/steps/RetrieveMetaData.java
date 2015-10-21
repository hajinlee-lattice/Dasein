package com.latticeengines.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractStep;

@Component("retrieveMetaData")
public class RetrieveMetaData extends AbstractStep {

    private static final Log log = LogFactory.getLog(RetrieveMetaData.class);

    @Override
    public void execute() {
        log.info("Inside RetrieveMetaData execute()");
    }

    public static class RetrieveMetadataConfiguration {

        private String a;
        private int b;
        private boolean c;

        public String getA() {
            return a;
        }
        public void setA(String a) {
            this.a = a;
        }
        public int getB() {
            return b;
        }
        public void setB(int b) {
            this.b = b;
        }
        public boolean isC() {
            return c;
        }
        public void setC(boolean c) {
            this.c = c;
        }

    }

}
