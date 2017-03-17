package com.tyaer.database.mongo.bean;

/**
 * Created by Twin on 2017/3/8.
 */
public class BsonBean {
    private Rule rule;
    private String key;
    private Object value;

    public BsonBean(Rule rule, String key, Object value) {

        this.rule = rule;
        this.key = key;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("BsonBean{");
        sb.append("rule=").append(rule);
        sb.append(", key='").append(key).append('\'');
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public Rule getRule() {
        return rule;
    }

    public void setRule(Rule rule) {
        this.rule = rule;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}
