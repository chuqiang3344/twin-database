package com.tyaer.db.mongo.cursor;

/**
 * Created by Twin on 2017/3/8.
 */

import com.mongodb.BasicDBObject;
import com.tyaer.db.mongo.bean.Sort;

/**
 * 分页,排序对象
 *
 * @author <a href="http://blog.csdn.net/java2000_wl">java2000_wl</a>
 * @version <b>1.0</b>
 */
public class PagingBean {

    /**
     * 第几页，跳过多少条记录
     */
    private int pageIndex;

    /**
     * 返回多少条记录
     */
    private int pageSize;

    private Sort sort;

    public PagingBean(int pageIndex, int pageSize) {
        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public Sort getSort() {
        return sort;
    }

    public void setSort(Sort sort) {
        this.sort = sort;
    }

    public BasicDBObject getSortObject() {
        if (this.sort == null) {
            return null;
        }
        return this.sort.getSortObject();
    }
}
