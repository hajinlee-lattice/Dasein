package com.latticeengines.domain.exposed.monitor;

public class SlackSettings {
    private String webHookUrl;

    private String title;

    private String pretext;

    private String text;

    private String userName;

    private Color color = Color.NORMAL;

    public SlackSettings(String webHookUrl, String title, String pretext, String text, String userName, Color color) {
        this.webHookUrl = webHookUrl;
        this.title = title;
        this.pretext = pretext;
        this.text = text;
        this.userName = userName;
        this.color = color;
    }

    public String getWebHookUrl() {
        return webHookUrl;
    }

    public void setWebHookUrl(String webHookUrl) {
        this.webHookUrl = webHookUrl;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getPretext() {
        return pretext;
    }

    public void setPretext(String pretext) {
        this.pretext = pretext;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Color getColor() {
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public enum Color {
        GOOD("good"), DANGER("danger"), NORMAL("");
        
        private String color;
        
        private Color(String color) {
            this.color = color;
        }

        public String getColor() {
            return color;
        }

    }

}
