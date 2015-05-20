package com.latticeengines.admin.service.impl;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.admin.service.FileSystemService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("fileSystemService")
public class FileSystemServiceImpl implements FileSystemService {

    @Override
    public List<String> filesInDirectory(File dir){
        try {
            return Arrays.asList(dir.list());
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19103, e);
        }
    }

    @Override
    public void deleteFile(File file) {
        try {
            FileUtils.deleteQuietly(file);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_19103, e);
        }
    }
}
