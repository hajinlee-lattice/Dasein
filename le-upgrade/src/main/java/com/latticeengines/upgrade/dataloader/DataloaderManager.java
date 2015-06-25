package com.latticeengines.upgrade.dataloader;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component("dataloaderManager")
public class DataloaderManager {

    @Value("${upgrade.dl.url.1}")
    private String dlUrl1;

    @Value("${upgrade.dl.url.2}")
    private String dlUrl2;

}
