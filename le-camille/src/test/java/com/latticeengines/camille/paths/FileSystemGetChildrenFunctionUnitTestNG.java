package com.latticeengines.camille.paths;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.camille.exposed.paths.FileSystemGetChildrenFunction;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.DocumentDirectory.Node;
import com.latticeengines.domain.exposed.camille.Path;

public class FileSystemGetChildrenFunctionUnitTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(new Object() {
    }.getClass().getEnclosingClass());

    private File tempDir;

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        tempDir = Files.createTempDir();

        createDirectory(tempDir + "/0");
        createDirectory(tempDir + "/0/1");
        createDirectory(tempDir + "/0/2");
        createDirectory(tempDir + "/0/1/3");
        createDirectory(tempDir + "/0/1/4");
        createDirectory(tempDir + "/0/2/5");
        createDirectory(tempDir + "/0/2/6");

        createTextFile(tempDir + "/0/0.txt", "zero");
        createTextFile(tempDir + "/0/1/1.txt", "one");
        createTextFile(tempDir + "/0/2/2.txt", "two");
        createTextFile(tempDir + "/0/1/3/3.txt", "three");
        createTextFile(tempDir + "/0/1/4/4.txt", "four");
        createTextFile(tempDir + "/0/2/5/5.txt", "five");
        createTextFile(tempDir + "/0/2/6/6.txt", "six");
    }

    private static void createDirectory(String path) {
        File dir = new File(path);
        dir.mkdir();
        dir.deleteOnExit();
    }

    private static void createTextFile(String path, String contents) throws FileNotFoundException {
        try (PrintWriter w = new PrintWriter(path)) {
            w.print(contents);
        }
        new File(path).deleteOnExit();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(tempDir);
    }

    @Test(groups = "unit")
    public void testRootIterationAndFileContents() throws IOException {
        FileSystemGetChildrenFunction func = new FileSystemGetChildrenFunction(tempDir);
        DocumentDirectory d = new DocumentDirectory(new Path("/"), func);
        Iterator<Node> iter = d.depthFirstIterator();
        for (String expected : new String[] { "/0", "/0/0.txt", "/0/1", "/0/1/1.txt", "/0/1/3", "/0/1/3/3.txt",
                "/0/1/4", "/0/1/4/4.txt", "/0/2", "/0/2/2.txt", "/0/2/5", "/0/2/5/5.txt", "/0/2/6", "/0/2/6/6.txt" }) {
            Node n = iter.next();
            Assert.assertEquals(n.getPath().toString(), expected);
            switch (n.getPath().toString()) {
            case "/0/0.txt":
                Assert.assertEquals("zero", n.getDocument().getData());
                break;
            case "/0/1/1.txt":
                Assert.assertEquals("one", n.getDocument().getData());
                break;
            case "/0/2/2.txt":
                Assert.assertEquals("two", n.getDocument().getData());
                break;
            case "/0/1/3/3.txt":
                Assert.assertEquals("three", n.getDocument().getData());
                break;
            case "/0/1/4/4.txt":
                Assert.assertEquals("four", n.getDocument().getData());
                break;
            case "/0/2/5/5.txt":
                Assert.assertEquals("five", n.getDocument().getData());
                break;
            case "/0/2/6/6.txt":
                Assert.assertEquals("six", n.getDocument().getData());
                break;
            }
        }
    }

    @Test(groups = "unit")
    public void testSubIteration() throws IOException {
        FileSystemGetChildrenFunction func = new FileSystemGetChildrenFunction(tempDir);
        DocumentDirectory d = new DocumentDirectory(new Path("/0/1"), func);
        Iterator<Node> iter = d.leafFirstIterator();
        for (String expected : new String[] { "/0/1/4/4.txt", "/0/1/3/3.txt", "/0/1/4", "/0/1/3", "/0/1/1.txt" }) {
            Assert.assertEquals(iter.next().getPath().toString(), expected);
        }
    }
}
