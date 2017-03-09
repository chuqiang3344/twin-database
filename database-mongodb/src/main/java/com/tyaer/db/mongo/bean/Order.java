package com.tyaer.db.mongo.bean;

/**
 * Created by Twin on 2017/3/8.
 */

/**
 * 排序规则
 * 1,表示按正序排序(即：从小到大排序)；－1,表示按倒序排序(即：从大到小排序)
 */
public enum Order {

    ASC(1), DESC(-1);

    int nCode;

    Order(int nCode) {
        this.nCode = nCode;
    }

    public int getnCode() {
        return nCode;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Order{");
        sb.append("nCode=").append(nCode);
        sb.append('}');
        return sb.toString();
    }
}
