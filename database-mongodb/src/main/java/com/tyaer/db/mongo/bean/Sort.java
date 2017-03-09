package com.tyaer.db.mongo.bean;

/**
 * Created by Twin on 2017/3/8.
 */

import com.mongodb.BasicDBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 排序对象
 *
 * @author <a href="http://blog.csdn.net/java2000_wl">java2000_wl</a>
 * @version <b>1.0</b>
 */
public class Sort {

    /**
     * key为排序的名称, value为顺序
     */
    private Map<String, Order> field = new LinkedHashMap<String, Order>();

    public Sort() {
    }

    public Sort(String key, Order order) {
        field.put(key, order);
    }

    public Sort on(String key, Order order) {
        field.put(key, order);
        return this;
    }

    public BasicDBObject getSortObject() {
        BasicDBObject dbo = new BasicDBObject();
        for (Map.Entry<String, Order> entry : field.entrySet()) {
            dbo.put(entry.getKey(), entry.getValue().nCode);
        }
//        for (String k : field.keySet()) {
////            dbo.put(k, (field.get(k).equals(Order.ASC) ? 1 : -1));
//            dbo.put(k, field.get(k).nCode);
//        }
        return dbo;
    }

    public Bson getSortObject1() {
        ArrayList<Bson> bsonArrayList = new ArrayList<>();
        BasicDBObject basicDBObject = new BasicDBObject();
        for (String k : field.keySet()) {
            basicDBObject.put(k, (field.get(k).equals(Order.ASC) ? 1 : -1));
        }
        return Filters.and(bsonArrayList);
    }

    public boolean isEmpty() {
        Map<String, Order> field = this.field;
        if (field.size() == 0) {
            return true;
        }
        return false;
    }
}
