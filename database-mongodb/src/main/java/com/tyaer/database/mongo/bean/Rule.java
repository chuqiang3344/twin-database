package com.tyaer.database.mongo.bean;

/**
 * Created by Twin on 2017/3/8.
 */
public enum Rule {
    EQ("等于"), REGEX("REGEX"), NE("不等于"), IN("包含于"), LT("小于"), GT("大于");

    String description;

    Rule(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Rule{");
        sb.append("description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
