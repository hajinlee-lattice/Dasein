package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRProperty;

public class PythonInvoker {

    private static final Log log = LogFactory.getLog(PythonMRUtils.class);
    private Classifier classifier;
    private String runtimeConfigFile;

    public PythonInvoker(Classifier classifier, String runtimeConfigFile) {
        this.classifier = classifier;
        this.runtimeConfigFile = runtimeConfigFile;
    }

    public void callLauncher(Configuration config) {
        int exitValue = 0;
        try {
            PythonMRUtils.writeMetadataJsonToLocal(classifier);
            runtimeConfigFile = runtimeConfigFile != null ? runtimeConfigFile : "None";
            ProcessBuilder pb = new ProcessBuilder().inheritIO().command("./pythonlauncher.sh", "lattice",
                    "launcher.py", "metadata.json", runtimeConfigFile);
            setupEnvironment(pb.environment(), config);

            Process p = pb.start();
            log.info("Python process started");
            p.waitFor();
            exitValue = p.exitValue();
            log.info("Python process finished successfully");
            PythonMRUtils.copyMetadataJsonToHdfs(config, classifier.getModelHdfsDir());

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_12011, e);
        }

        if (exitValue != 0) {
            throw new LedpException(LedpCode.LEDP_12011, new String[] { "Python exited with " + exitValue });
        }

    }

    private void setupEnvironment(Map<String, String> env, Configuration config) {
        env.put(PythonMRProperty.PYTHONPATH.name(), config.get(PythonMRProperty.PYTHONPATH.name()));
        env.put(PythonMRProperty.SHDP_HD_FSWEB.name(), config.get(PythonMRProperty.SHDP_HD_FSWEB.name()));
        env.put(PythonMRProperty.PYTHONIOENCODING.name(), config.get(PythonMRProperty.PYTHONIOENCODING.name()));
        env.put(PythonMRProperty.DEBUG.name(), config.get(PythonMRProperty.DEBUG.name()));
    }
}
