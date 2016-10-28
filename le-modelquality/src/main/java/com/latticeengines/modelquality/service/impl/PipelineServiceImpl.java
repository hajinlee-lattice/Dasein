package com.latticeengines.modelquality.service.impl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
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
    private static final Log log = LogFactory.getLog(PipelineServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;
    
    @Autowired
    private PipelineEntityMgr pipelineEntityMgr;
    
    @Autowired
    private PipelineStepEntityMgr pipelineStepEntityMgr;
    
    @Autowired
    private PipelineToPipelineStepsEntityMgr pipelineToPipelineStepsEntityMgr;
    
    @Override
    public Pipeline createLatestProductionPipeline() {
        String version = getVersion();
        String pipelineName = "PRODUCTION-" + version;
        Pipeline pipeline = pipelineEntityMgr.findByName(pipelineName);
        
        if(pipeline != null)
        {
            return pipeline;
        }
        
        pipeline = new Pipeline();
        pipeline.setName("PRODUCTION-" + version);
        pipeline.setDescription("Production pipeline version: " + pipeline.getName());
        String pipelineJson = String.format("/app/%s/dataplatform/scripts/pipeline.json", version);
        setPipelineProperties(pipeline, pipelineJson);
        
        try {
            String pipelineContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, pipelineJson);
            pipeline.addStepsFromPipelineJson(pipelineContents);
            pipelineEntityMgr.create(pipeline);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return pipeline;
    }
    
    @Override
    public String uploadPipelineStepFile(String stepName, InputStream inputStream, String[] names, PipelineStepType type) {
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
    public Pipeline createPipeline(String pipelineName, String pipelineDescription, List<PipelineStepOrFile> pipelineSteps) {
        Pipeline pipeline = new Pipeline();
        pipeline.setName(pipelineName);
        pipeline.setDescription(pipelineDescription);
        pipelineEntityMgr.create(pipeline);
        int i = 1;
        for (PipelineStepOrFile psof : pipelineSteps) {
            PipelineToPipelineSteps p = new PipelineToPipelineSteps();
            p.setPipeline(pipeline);
            p.setOrder(i++);
            PipelineStep step = null;
            if (psof.pipelineStepName != null) {
                step = pipelineStepEntityMgr.findByName(psof.pipelineStepName);
                
                if (step == null) {
                    throw new RuntimeException(String.format("Pipeline step with name %s does not exist", psof.pipelineStepName));
                }
            } else if (psof.pipelineStepDir != null) {
                try {
                    String pipelineStepMetadata = HdfsUtils.getHdfsFileContents(yarnConfiguration, //
                            psof.pipelineStepDir + "/metadata.json");
                    
                    step = JsonUtils.deserialize(pipelineStepMetadata, PipelineStep.class);
                    String[] pipelineStepPythonScripts = getPythonScripts(step.getName(), psof.pipelineStepDir);
                    step.setScript(pipelineStepPythonScripts[0]);
                    step.setRtsScript(pipelineStepPythonScripts[1]);
                    step.setLoadFromHdfs(true);
                    pipelineStepEntityMgr.create(step);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            p.setPipelineStep(step);
            pipelineToPipelineStepsEntityMgr.create(p);
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
        String version = getVersion();
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
            s.setSortKey(i);
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
            List<String> pythonFiles = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsPath, new HdfsUtils.HdfsFilenameFilter() {
                
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
