package org.springframework.yarn.fs;

import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;

public class PrototypeLocalResourcesFactoryBean extends LocalResourcesFactoryBean {

    @Override
    public boolean isSingleton() {
        return false;
    }

    public static class TransferEntry extends org.springframework.yarn.fs.LocalResourcesFactoryBean.TransferEntry {
        public TransferEntry() {
            super(null, null, null, false);
        }

        public void setType(LocalResourceType type) {
            this.type = type;
        }

        public void setVisibility(LocalResourceVisibility visibility) {
            this.visibility = visibility;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public void setStaging(boolean staging) {
            this.staging = staging;
        }
    }

    public static class CopyEntry extends org.springframework.yarn.fs.LocalResourcesFactoryBean.CopyEntry {
        public CopyEntry(String src, String dest, boolean staging) {
            super(src, dest, staging);
        }

        public CopyEntry() {
            super(null, null, false);
        }

        public void setSrc(String src) {
            this.src = src;
        }

        public void setDest(String dest) {
            this.dest = dest;
        }

        public void setStaging(boolean staging) {
            this.staging = staging;
        }

        public String getSrc() {
            return src;
        }

        public String getDest() {
            return dest;
        }

        public boolean getStaging() {
            return staging;
        }
    }
}
