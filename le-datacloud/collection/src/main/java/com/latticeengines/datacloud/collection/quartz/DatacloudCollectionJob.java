package com.latticeengines.datacloud.collection.quartz;

import java.util.concurrent.Callable;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.service.CollectionDBService;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;

@Component("datacloudCollectionJob")
public class DatacloudCollectionJob implements QuartzJobBean {

    @Inject
    CollectionDBService collectionDBService;

    public Callable<Boolean> getCallable(String jobArguments) {

        return new Callable<Boolean>() {

            @Override
            public Boolean call() {

                collectionDBService.collect();

                return true;

            }

        };

    }

}
