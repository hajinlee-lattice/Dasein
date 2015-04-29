package com.latticeengines.admin.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.python.icu.impl.IllegalIcuArgumentException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.FileCopyUtils;

import com.latticeengines.admin.service.ServerFileService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("serverFileService")
public class ServerFileServiceImpl implements ServerFileService {

    @Value("${admin.config.rootpath}")
    private String rootPath;

    @Override
    public void downloadFile(HttpServletRequest request, HttpServletResponse response,
                             String path, String filename, String mimeType) {

        String fullPath = rootPath + path;
        try {
            if (isValidPath(path)) {
                response.setContentType(mimeType);
                if (filename == null) { filename = getSuffix(path); }
                response.setHeader("Content-Disposition", String.format("attachment; filename=\"%s\"", filename));
                InputStream is = new FileInputStream(new File(fullPath));
                FileCopyUtils.copy(is, response.getOutputStream());
            }
        } catch (FileNotFoundException e) {
            String host = request.getRemoteHost();
            if (host == null) host = "";
            throw new LedpException(LedpCode.LEDP_00003, new String[]{fullPath, host});
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00003,
                    "Failed to open an output stream for the HttpServletResponse", e);
        }
    }

    private static boolean isValidPath(String path) {
        if (path == null || path.length() < 2) {
            IllegalArgumentException e =
                    new IllegalIcuArgumentException("Path cannot be empty.");
            throw new LedpException(LedpCode.LEDP_00003, e);
        }
        if (!path.startsWith("/")) {
            IllegalArgumentException e =
                    new IllegalIcuArgumentException("Path must start with /");
            throw new LedpException(LedpCode.LEDP_00003, e);
        }
        if (path.contains("/../") || path.startsWith("../") || path.endsWith("/..") || path.equals("..")) {
            IllegalArgumentException e =
                    new IllegalIcuArgumentException("Using \"..\" to access parent directory is forbidden.");
            throw new LedpException(LedpCode.LEDP_00003, e);
        }
        return true;
    }

    private static String getSuffix(String path) {
        int lastSlash = path.lastIndexOf("/");
        if (lastSlash == -1) return path;

        return path.substring(lastSlash, path.length());
    }

    @Override
    public String getRootPath() { return rootPath; }

}
