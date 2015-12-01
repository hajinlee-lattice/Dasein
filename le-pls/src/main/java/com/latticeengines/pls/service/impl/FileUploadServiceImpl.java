package com.latticeengines.pls.service.impl;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.service.FileUploadService;

@Component("fileUploadService")
public class FileUploadServiceImpl implements FileUploadService {
    
    //@Value("${pls.upload.dest.dir}")
    private String destDir;

    @Override
    public void uploadFile(String outputFileName, InputStream fileInputStream) {
        int read = 0;
        byte[] bytes = new byte[1024];
        try (OutputStream out = new FileOutputStream(new File(destDir + "/" + outputFileName))) {
            while ((read = fileInputStream.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18053, e);
        }
    }

}
