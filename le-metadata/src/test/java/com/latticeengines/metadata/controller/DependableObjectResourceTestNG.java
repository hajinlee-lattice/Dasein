package com.latticeengines.metadata.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.ArrayList;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DependableObject;
import com.latticeengines.metadata.functionalframework.MetadataFunctionalTestNGBase;
import com.latticeengines.proxy.exposed.metadata.DependableObjectProxy;

public class DependableObjectResourceTestNG extends MetadataFunctionalTestNGBase {

    @Autowired
    private DependableObjectProxy dependableObjectProxy;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        super.setup();
    }

    @Test(groups = "functional")
    public void createDependableObject() {
        DependableObject child = createExampleDependableObject("Child");
        dependableObjectProxy.createOrUpdate(CUSTOMERSPACE1, child);

        DependableObject parent = createExampleDependableObject("Parent");
        parent.addDependency(child);
        dependableObjectProxy.createOrUpdate(CUSTOMERSPACE1, parent);

        DependableObject retrieved = dependableObjectProxy.find(CUSTOMERSPACE1, parent.getType(), parent.getName());
        assertNotNull(retrieved);
        assertEquals(retrieved.getName(), parent.getName());
        assertEquals(retrieved.getType(), parent.getType());
        assertEquals(retrieved.getDependencies().size(), 1);
    }

    @Test(groups = "functional", dependsOnMethods = "createDependableObject")
    public void updateDependableObject() {
        DependableObject example = createExampleDependableObject("Parent");
        DependableObject retrieved = dependableObjectProxy.find(CUSTOMERSPACE1, example.getType(), example.getName());
        retrieved.setType("Foobar");
        retrieved.setDependencies(new ArrayList<>());
        dependableObjectProxy.createOrUpdate(CUSTOMERSPACE1, retrieved);

        retrieved = dependableObjectProxy.find(CUSTOMERSPACE1, "Foobar", "Parent");
        assertEquals(retrieved.getDependencies().size(), 0);
    }

    @Test(groups = "functional", dependsOnMethods = "updateDependableObject")
    public void deleteDependableObject() {
        DependableObject object = createExampleDependableObject("Parent");
        dependableObjectProxy.delete(CUSTOMERSPACE1, object.getType(), object.getName());
        DependableObject retrieved = dependableObjectProxy.find(CUSTOMERSPACE1, object.getType(), object.getName());
        assertNull(retrieved);
    }

    private DependableObject createExampleDependableObject(String name) {
        DependableObject dependableObject = new DependableObject();
        dependableObject.setName(name);
        dependableObject.setType(DependableObject.class.getTypeName());
        return dependableObject;
    }

}
