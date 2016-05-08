package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.Map;

public enum ArtifactType {
    
    PMML("pmmlfile", "PMMLFiles", "xml"),
    PivotMapping("pivotmapping", "PivotMappings", "csv"),
    Function("pythonmodule", "PythonModules", "py");
    
    private String code;
    private String pathToken;
    private String fileType;
    
    private static Map<String, ArtifactType> codeToArtifactTypeMap = new HashMap<>();
    private static Map<String, ArtifactType> urlTokenToArtifactTypeMap = new HashMap<>();
    static {
        for (ArtifactType artifactType : ArtifactType.values()) {
            codeToArtifactTypeMap.put(artifactType.getCode(), artifactType);
            urlTokenToArtifactTypeMap.put(artifactType.getUrlToken(), artifactType);
        }
    }

    ArtifactType(String code, String pathToken, String fileType) {
        this.code = code;
        this.pathToken = pathToken;
        this.fileType = fileType;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getUrlToken() {
        return code + "s";
    }
    
    public String getPathToken() {
        return pathToken;
    }
    
    public String getFileType() {
        return fileType;
    }
    
    public static ArtifactType getArtifactTypeByCode(String code) {
        return codeToArtifactTypeMap.get(code);
    }

    public static ArtifactType getArtifactTypeByUrlToken(String urlToken) {
        return urlTokenToArtifactTypeMap.get(urlToken);
    }
}
