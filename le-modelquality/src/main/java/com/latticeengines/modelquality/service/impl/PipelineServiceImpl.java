package com.latticeengines.modelquality.service.impl;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelineJson;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.PipelineStepOrFile;
import com.latticeengines.domain.exposed.modelquality.PipelineToPipelineSteps;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineStepEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineToPipelineStepsEntityMgr;
import com.latticeengines.modelquality.service.PipelineService;

@Component("pipelineService")
public class PipelineServiceImpl extends BaseServiceImpl implements PipelineService {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PipelineServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;

    @Autowired
    private PipelineStepEntityMgr pipelineStepEntityMgr;

    @Autowired
    private PipelineToPipelineStepsEntityMgr pipelineToPipelineStepsEntityMgr;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    private final String pipelineJson = "/datascience/%s/dataplatform/scripts/pipeline.json";

    @Override
    public Pipeline createLatestProductionPipeline() {
        String version = getLedsVersion();
        String pipelineName = "PRODUCTION-" + version.replace('/', '_');
        Pipeline pipeline = pipelineEntityMgr.findByName(pipelineName);

        if (pipeline != null) {
            return pipeline;
        }

        pipeline = new Pipeline();
        pipeline.setName(pipelineName);
        pipeline.setDescription("Production pipeline version: " + pipeline.getName());
        String pipelineJsonPath = String.format(pipelineJson, version);
        setPipelineProperties(pipeline, pipelineJsonPath);

        try {
            String pipelineContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, pipelineJsonPath);
            pipeline.addStepsFromPipelineJson(pipelineContents);

            for (PipelineStep ps : pipeline.getPipelineSteps()) {
                PipelineStep pStep = pipelineStepEntityMgr.findByName(ps.getName());
                if (pStep == null) {
                    pipelineStepEntityMgr.create(ps);
                } else {
                    ps.setPid(pStep.getPid());
                    pipelineStepEntityMgr.update(ps);
                }
            }

            Pipeline previousLatest = pipelineEntityMgr.getLatestProductionVersion();
            int versionNo = 1;
            if (previousLatest != null) {
                versionNo = previousLatest.getVersion() + 1;
            }
            pipeline.setVersion(versionNo);

            pipelineEntityMgr.create(pipeline);
        } catch (FileNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_35007, new String[] { pipelineJsonPath });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return pipeline;
    }

    @Override
    public String uploadPipelineStepFile(String stepName, InputStream inputStream, String[] names, PipelineStepType type) {

        if (pipelineStepEntityMgr.findByName(stepName) != null) {
            throw new LedpException(LedpCode.LEDP_35002, new String[] { "Pipeline Step", stepName });
        }

        try {
            switch (type) {
            case METADATA:
                names[0] = "metadata";
                break;
            case PYTHONLEARNING:
                names[0] = stepName;
                break;
            case PYTHONRTS:
                break;
            }
            HdfsUtils.mkdir(yarnConfiguration, getHdfsDir() + "/steps/" + stepName);
            String tplPath = "%s/steps/%s/%s.%s";
            String path = String.format(tplPath, getHdfsDir(), stepName, names[0], names[1]);
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, path);
            return path;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Pipeline createPipeline(String pipelineName, String pipelineDescription,
            List<PipelineStepOrFile> pipelineSteps) {
        Pipeline pipeline = new Pipeline();
        List<PipelineToPipelineSteps> pToPSteps = new ArrayList<PipelineToPipelineSteps>();
        pipeline.setName(pipelineName);
        pipeline.setDescription(pipelineDescription);
        int i = 1;
        for (PipelineStepOrFile psof : pipelineSteps) {
            PipelineToPipelineSteps ptoPStep = new PipelineToPipelineSteps();
            ptoPStep.setPipeline(pipeline);
            ptoPStep.setOrder(i++);
            PipelineStep step = null;
            if (psof.pipelineStepName != null) {
                step = pipelineStepEntityMgr.findByName(psof.pipelineStepName);
                if (step == null) {
                    throw new LedpException(LedpCode.LEDP_35000,
                            new String[] { "Pipeline step", psof.pipelineStepName });
                }
            } else if (psof.pipelineStepDir != null) {
                try {
                    String pipelineStepMetadata = HdfsUtils.getHdfsFileContents(yarnConfiguration, psof.pipelineStepDir
                            + "/metadata.json");

                    step = JsonUtils.deserialize(pipelineStepMetadata, PipelineStep.class);
                    String[] pipelineStepPythonScripts = getPythonScripts(step.getName(), psof.pipelineStepDir);
                    step.setScript(pipelineStepPythonScripts[0]);
                    step.setRtsScript(pipelineStepPythonScripts[1]);
                    step.setLoadFromHdfs(true);
                    pipelineStepEntityMgr.create(step);
                } catch (IOException e) {
                    throw new LedpException(LedpCode.LEDP_35001, new String[] { "" });
                }
            }
            ptoPStep.setPipelineStep(step);
            pToPSteps.add(ptoPStep);
        }

        // Now that steps have been validated and created, create the pipeline
        // and the associations
        pipelineEntityMgr.create(pipeline);
        for (PipelineToPipelineSteps ptoPStep : pToPSteps) {
            pipelineToPipelineStepsEntityMgr.create(ptoPStep);
        }

        Pipeline p = pipelineEntityMgr.findByName(pipelineName);
        try {
            String pipelineJsonHdfsPath = createPipelineInHdfs(p);
            setPipelineProperties(p, pipelineJsonHdfsPath);
            pipelineEntityMgr.update(p);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return p;
    }

    private void setPipelineProperties(Pipeline pipeline, String pipelineJson) {
        String version = getLedsVersion();
        String pythonLibScript = String.format("/app/%s/dataplatform/scripts/lepipeline.tar.gz", version);
        String pipelineScript = String.format("/app/%s/dataplatform/scripts/pipeline.py", version);

        pipeline.setPipelineLibScript(pythonLibScript);
        pipeline.setPipelineDriver(pipelineJson);
        pipeline.setPipelineScript(pipelineScript);
    }

    private String createPipelineInHdfs(Pipeline pipeline) throws IOException {
        List<PipelineStep> steps = pipeline.getPipelineSteps();
        Map<String, PipelineStep> stepsMap = new HashMap<>();
        for (int i = 1; i <= steps.size(); i++) {
            PipelineStep s = steps.get(i - 1);
            stepsMap.put(s.getMainClassName(), s);
        }
        PipelineJson pipelineJson = new PipelineJson(stepsMap);
        String pipelineContents = JsonUtils.serialize(pipelineJson);
        String pipelineName = pipeline.getName();
        HdfsUtils.mkdir(yarnConfiguration, getHdfsDir() + "/pipelines/" + pipelineName);
        String tplPath = "%s/pipelines/%s/pipeline-%d.json";
        String path = String.format(tplPath, getHdfsDir(), pipelineName, System.currentTimeMillis());
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, new ByteArrayInputStream(pipelineContents.getBytes()), path);
        return path;
    }

    private String[] getPythonScripts(String stepName, String hdfsPath) {
        try {
            List<String> pythonFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsPath,
                    new HdfsUtils.HdfsFilenameFilter() {

                        @Override
                        public boolean accept(String filename) {
                            return filename.endsWith(".py");
                        }
                    });
            String[] files = new String[2];

            for (String pythonFile : pythonFiles) {
                String pyHdfsPath = Path.getPathWithoutSchemeAndAuthority(new Path(pythonFile)).toString();
                if (pythonFile.endsWith(stepName + ".py")) {
                    files[0] = pyHdfsPath;
                } else {
                    files[1] = pyHdfsPath;
                }
            }
            return files;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
