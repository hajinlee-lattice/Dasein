package com.latticeengines.domain.exposed.monitor;

public class MsTeamsSettings {
    private String webHookUrl;

    private String title;

    private String text;

    private String pretext;

    private Color themeColor;

    public MsTeamsSettings(String webHookUrl, String title, String pretext, String text,
            Color themeColor) {
        this.webHookUrl = webHookUrl;
        this.title = title;
        this.text = text;
        this.pretext = pretext;
        this.themeColor = themeColor;
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

    public Color getColor() {
        return themeColor;
    }

    public void setColor(Color themeColor) {
        this.themeColor = themeColor;
    }

    public enum Color {
        GOOD("0072C6"), DANGER("e61034"), NORMAL("");

        private String color;

        Color(String color) {
            this.color = color;
        }

        public String getColor() {
            return color;
        }

    }

}
