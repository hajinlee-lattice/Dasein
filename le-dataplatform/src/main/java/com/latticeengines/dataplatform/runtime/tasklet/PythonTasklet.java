package com.latticeengines.dataplatform.runtime.tasklet;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

public class PythonTasklet implements Tasklet {

    private static final Log log = LogFactory.getLog(PythonTasklet.class);

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Process p = Runtime.getRuntime().exec(
                "/usr/local/bin/python2.7 launcher.py metadata.json runtimeconfig.properties");
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
            String line = "";
            while ((line = reader.readLine()) != null) {
                log.info(line);
            }
            p.waitFor();
            int exitValue = p.exitValue();
            if (exitValue != 0) {
                throw new LedpException(LedpCode.LEDP_15004, new String[] { exitValue + "" });
            }
        }
        return RepeatStatus.FINISHED;
    }

}
