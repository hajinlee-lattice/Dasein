package com.latticeengines.upgrade.domain;

public class BardInfo {
 
    private String displayName;

    private String instance;

    private String name;

    private String settings;

    public String getDisplayName(){
        return displayName;
    }

    public void setDisplayName(String displayName){
        this.displayName = displayName;
    }

    public String getInstance(){
        return instance;
    }

    public void setInstance(String instance){
        this.instance = instance;
    }

    public String getName(){
        return name;
    }

    public void setName(String name){
        this.name = name;
    }

    public String getSettings(){
        return settings;
    }

    public void setSettings(String settings){
        this.settings = settings;
    }
}
