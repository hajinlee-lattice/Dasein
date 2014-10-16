package com.latticeengines.domain.exposed.camille;

import java.io.Serializable;

public class Document implements Serializable {
    private static final long serialVersionUID = 1L;

    private String data;
    private int version = -1;

    public Document() {
    }

    public Document(String data) {
        this.data = data;
    }

    public String getData() {
        return data == null ? "" : data;
    }

    public void setData(String data) {
        this.data = data;
    }

    public int getVersion() {
        return this.version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public boolean versionSpecified() {
        return this.version >= 0;
    }

    @Override
    public String toString() {
        return "Document [data=" + data + ", version=" + version + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((data == null) ? 0 : data.hashCode());
        result = prime * result + version;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Document other = (Document) obj;
        if (data == null) {
            if (other.data != null)
                return false;
        } else if (!data.equals(other.data))
            return false;
        if (version != other.version)
            return false;
        return true;
    }

}