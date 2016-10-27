package com.latticeengines.sampleapi.sample.service;

import java.util.List;

public interface SampleService {

    Object doSampleWork(Object matchRequest) throws Exception;

    List<Object> doSampleWork(List<Object> matchRequests) throws Exception;

}
