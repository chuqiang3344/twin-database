package com.tyaer.db.mongo.old;

/**
 * Created by Twin on 2017/3/8.
 */

import com.mongodb.DBCursor;

/**
 * 分页,排序处理
 *
 * @author <a href="http://blog.csdn.net/java2000_wl">java2000_wl</a>
 * @version <b>1.0</b>
 */
public interface CursorPreparer {

    DBCursor prepare(DBCursor cursor);
}
