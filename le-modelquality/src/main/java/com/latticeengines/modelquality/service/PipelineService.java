package com.latticeengines.modelquality.service;

import org.springframework.web.multipart.MultipartFile;

public interface PipelineService {

    String uploadFile(String fileName, MultipartFile file);

}
