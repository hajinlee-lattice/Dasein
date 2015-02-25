package com.latticeengines.pls.service.impl;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.pls.service.DataFileProviderService;

@Component("dataFileProviderService")
public class DataFileProviderServiceImpl implements DataFileProviderService {
    
    @Autowired
    private Configuration yarnConfiguration;
    
    
}
