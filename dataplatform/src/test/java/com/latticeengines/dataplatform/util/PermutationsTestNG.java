package com.latticeengines.dataplatform.util;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.testng.Assert;


public class PermutationsTestNG {

    private int numPermutationsExpected;
    private int permutationSizeExpected;
    
    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        InputStream in = Permutations.class.getClassLoader().
                getResourceAsStream("com/latticeengines/dataplatform/util/Permutations/perms.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        numPermutationsExpected = Integer.parseInt(br.readLine());
        permutationSizeExpected = Integer.parseInt(br.readLine());
        // XXX - read file with independent code to identify some elements for
        //   the getPermutationElement() call below
    }
    
  @Test(groups = "unit")
  public void getInstance() throws Exception {
      Permutations permutations = Permutations.getInstance();
      Assert.assertNotNull(permutations);
  }

  @Test(groups = "unit")
  public void getNumPermutations() {
      Permutations permutations = Permutations.getInstance();
      Assert.assertEquals(permutations.getNumPermutations().intValue(), numPermutationsExpected);
  }

  public void getPermutationElement() {
    throw new RuntimeException("Test not implemented");
  }

  @Test(groups = "unit")
  public void getPermutationSize() {
      Permutations permutations = Permutations.getInstance();
      Assert.assertEquals(permutations.getPermutationSize().intValue(), permutationSizeExpected);
  }
}
