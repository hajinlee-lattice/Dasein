package com.latticeengines.upgrade.dataloader;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("dataloaderManager")
public class DataloaderManager {

    @Value("${upgrade.dl.url}")
    private String dlUrl;

}
