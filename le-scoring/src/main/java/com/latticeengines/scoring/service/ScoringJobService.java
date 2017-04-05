package com.latticeengines.scoring.service;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;

public interface ScoringJobService {

    ApplicationId score(ScoringConfiguration scoringConfig);

    ApplicationId score(Properties properties);

    void setConfiguration(Configuration yarnConfiguration);

    Properties generateCustomizedProperties(ScoringConfiguration scoringConfig);

}
