package com.latticeengines.dellebi.util;

public interface FileSystemOperations {
    
    public void cleanFolder(String folderName);
    
    public int listFileNumber(String folderName);
    
    public boolean isEmpty(String folderName);

}
