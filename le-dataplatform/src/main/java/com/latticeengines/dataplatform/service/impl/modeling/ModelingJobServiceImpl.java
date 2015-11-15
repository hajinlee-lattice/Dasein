package com.latticeengines.dataplatform.service.impl.modeling;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobNameService;
import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.impl.JobServiceImpl;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelingJob;

@Component("modelingJobService")
public class ModelingJobServiceImpl extends JobServiceImpl implements ModelingJobService {

    @Autowired
    private AsyncTaskExecutor sqoopJobTaskExecutor;

    @Autowired
    private MetadataService metadataService;

    @Autowired
    protected ModelEntityMgr modelEntityMgr;

    @Autowired
    protected ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @Autowired
    protected JobNameService jobNameService;

    @Autowired
    protected JobEntityMgr jobEntityMgr;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId submitJob(ModelingJob modelingJob) {
        ApplicationId appId = sumbitJobInternal(modelingJob);
        modelingJob.setId(appId.toString());
        Model model = ((ModelingJob) modelingJob).getModel();

        ModelDefinition modelDefinition = model.getModelDefinition();
        // find the model def. already setup; model def is expected to be
        // pre-setup by user
        ModelDefinition predefinedModelDef = modelDefinitionEntityMgr.findByName(modelDefinition.getName());
        if (predefinedModelDef != null) {
            // associate persisted model def with model.
            model.setModelDefinition(predefinedModelDef);
        } else {
            // TODO: this should not be needed; since the way how it works is
            // that model def is already created in persistence
            modelDefinitionEntityMgr.create(modelDefinition);
            model.setModelDefinition(modelDefinition);
        }
        // create the model given the associated definition
        modelEntityMgr.createOrUpdate(model);
        jobEntityMgr.create(modelingJob);
        return appId;
    }

    protected ApplicationId sumbitJobInternal(ModelingJob modelingJob) {
        return super.submitJob(modelingJob);
    }

    @Override
    public JobStatus getJobStatus(String applicationId) {
        ModelingJob leafJob = getLeafJob(applicationId);
        JobStatus jobStatus = new JobStatus();
        if (leafJob != null) {
            applicationId = leafJob.getId();
            String classifierStr = (String) leafJob.getContainerPropertiesObject().get(
                    PythonContainerProperty.METADATA_CONTENTS.name());
            if (classifierStr != null) {
                Classifier classifier = JsonUtils.deserialize(classifierStr, Classifier.class);
                if (classifier != null) {
                    String[] tokens = StringUtils.split(applicationId, "_");
                    String folder = StringUtils.join(new String[] { tokens[1], tokens[2] }, "_");
                    jobStatus.setResultDirectory(classifier.getModelHdfsDir() + "/" + folder);
                    jobStatus.setDataDiagnosticsPath(classifier.getDataDiagnosticsPath());
                }
            }
        }
        populateJobStatusFromYarnAppReport(jobStatus, applicationId);
        return jobStatus;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public ApplicationId resubmitPreemptedJob(ModelingJob resubmitJob) {
        if (resubmitJob.getChildIdList().size() > 0) {
            if (log.isDebugEnabled()) {
                log.debug("Did not resubmit preempted job " + resubmitJob.getId() + ". Already resubmitted.");
            }
            return null;
        }
        Long parentId = resubmitJob.getPid();
        String metadata = resubmitJob.getContainerPropertiesObject().getProperty(
                PythonContainerProperty.METADATA_CONTENTS.name());

        ModelingJob newModelingJob = new ModelingJob();
        newModelingJob.setParentPid(parentId);
        newModelingJob.setModel(resubmitJob.getModel());
        newModelingJob.setClient(resubmitJob.getClient());
        newModelingJob.setAppMasterPropertiesObject(resubmitJob.getAppMasterPropertiesObject());
        newModelingJob.setContainerPropertiesObject(resubmitJob.getContainerPropertiesObject());
        newModelingJob.getContainerPropertiesObject().setProperty(PythonContainerProperty.METADATA.name(), metadata);
        // submit job to yarn and persist metadata
        ApplicationId appId = submitJob(newModelingJob);
        jobEntityMgr.createOrUpdate(newModelingJob);
        if (log.isInfoEnabled()) {
            log.info("Resubmitted job pid(" + parentId + ") and received new appId(" + newModelingJob.getId()
                    + ") in queue "
                    + newModelingJob.getAppMasterPropertiesObject().getProperty(AppMasterProperty.QUEUE.name()) + ".");
        }
        resubmitJob.addChildId(newModelingJob.getId());
        jobEntityMgr.update(resubmitJob);
        return appId;
    }

    protected ModelingJob getLeafJob(String applicationId) {
        ModelingJob modelingJob = (ModelingJob) jobEntityMgr.findByObjectId(applicationId); // /
        // jobEntityMgr.getById(applicationId);
        while (modelingJob != null && modelingJob.getChildIdList().size() > 0) {
            applicationId = modelingJob.getChildIdList().get(0);
            modelingJob = (ModelingJob) jobEntityMgr.findByObjectId(applicationId);
        }
        return modelingJob;
    }

}
