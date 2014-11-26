package com.latticeengines.datascience.utils;

import static org.testng.Assert.assertTrue;

import java.io.File;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ModelingSchemaJsonGeneratorUnitTestNG {
    
    private Class<?>[] classes = ModelingSchemaJsonGenerator.getClassesToGenerate();
    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        for (Class<?> c : classes) {
            File f = new File(c.getSimpleName().toLowerCase() + ".json");
            if (f.exists()) {
                f.delete();
            }
        }
    }

    @Test(groups = "unit")
    public void main() throws Exception {
        ModelingSchemaJsonGenerator.main(new String[] {});
        
        for (Class<?> c : classes) {
            assertTrue(new File(c.getSimpleName().toLowerCase() + ".json").exists());
        }
    }

}
