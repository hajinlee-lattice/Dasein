package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.latticeengines.domain.exposed.dataflow.operations.BitCodeBook;

public class DecodedPair {

    private BitCodeBook bitCodeBook;
    private List<String> decodedColumns;

    public DecodedPair() {
        super();
    }

    public DecodedPair(BitCodeBook bitCodeBook, List<String> decodedColumns) {
        this.bitCodeBook = bitCodeBook;
        this.decodedColumns = decodedColumns;
    }

    public BitCodeBook getBitCodeBook() {
        return bitCodeBook;
    }

    public List<String> getDecodedColumns() {
        return decodedColumns;
    }

}
