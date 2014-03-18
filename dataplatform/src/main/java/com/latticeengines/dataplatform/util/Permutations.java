package com.latticeengines.dataplatform.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;

public final class Permutations {

    private static final Log log = LogFactory.getLog(Permutations.class);

    private static Permutations instance = null;

    private Integer[][] randomPermutations;

    private Permutations() {

        log.debug("loading list of permutations");
        BufferedReader br = null;
        try {
            InputStream in = Permutations.class.getClassLoader().getResourceAsStream(
                    "com/latticeengines/dataplatform/util/permutations/perms.txt");
            br = new BufferedReader(new InputStreamReader(in));
            Integer numPermutations = Integer.parseInt(br.readLine());
            Integer numElements = Integer.parseInt(br.readLine());

            if (log.isDebugEnabled()) {
                log.debug("numPermutations=" + numPermutations + " numElements=" + numElements);
            }

            randomPermutations = new Integer[numPermutations][numElements];
            String line = null;
            Integer i = 0;
            while ((line = br.readLine()) != null) {
                String[] permutation = line.split(" ");
                for (int k = 0; k < permutation.length; k++) {
                    randomPermutations[i][k] = Integer.parseInt(permutation[k]);
                }
                i++;
            }
        } catch (Exception e) {
            log.error("Unable to read list of permutations.", e);
            throw new LedpException(LedpCode.LEDP_00002);
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception e) {
            }
        }
    }

    public static Permutations getInstance() {
        if (instance == null) {
            synchronized (Permutations.class) {
                instance = new Permutations();
            }
        }
        return instance;
    }

    public Integer getNumPermutations() {
        return randomPermutations.length;
    }

    public Integer getPermutationSize() {
        return randomPermutations[0].length;
    }

    // XXX - another way of enforcing constness?
    public Integer getPermutationElement(Integer id, Integer elementId) {
        return randomPermutations[id][elementId];
    }
}
