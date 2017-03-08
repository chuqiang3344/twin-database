package com.tyaer.db.mongo.old;

/**
 * Created by Twin on 2017/3/8.
 */

import com.mongodb.DBObject;

/**
 * 分页,排序对象
 *
 * @author <a href="http://blog.csdn.net/java2000_wl">java2000_wl</a>
 * @version <b>1.0</b>
 */
public class CursorObject {

    private int skip;

    private int limit;

    private Sort sort;

    public CursorObject skip(int skip) {
        this.skip = skip;
        return this;
    }

    public CursorObject limit(int limit) {
        this.limit = limit;
        return this;
    }

    public int getSkip() {
        return skip;
    }

    public int getLimit() {
        return limit;
    }

    public Sort sort() {
        if (this.sort == null) {
            this.sort = new Sort();
        }
        return this.sort;
    }

    public DBObject getSortObject() {
        if (this.sort == null) {
            return null;
        }
        return this.sort.getSortObject();
    }
}
