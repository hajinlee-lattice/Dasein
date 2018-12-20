package com.latticeengines.spark.exposed.service;

import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.SparkScript;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;

public interface SparkJobService {

    SparkJobResult runScript(LivySession session, SparkScript script, ScriptJobConfig config);

    <J extends AbstractSparkJob<C>, C extends SparkJobConfig> //
    SparkJobResult runJob(LivySession session, Class<J> jobClz, C jobConfig);

}
