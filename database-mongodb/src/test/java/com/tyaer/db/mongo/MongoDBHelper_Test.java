package com.tyaer.db.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.result.DeleteResult;
import com.tyaer.db.mongo.bean.BsonBean;
import com.tyaer.db.mongo.bean.Order;
import com.tyaer.db.mongo.bean.Rule;
import com.tyaer.db.mongo.bean.Sort;
import com.tyaer.db.mongo.cursor.PagingBean;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;

/**
 * Created by Twin on 2017/3/8.
 */
public class MongoDBHelper_Test {
    MongoDBHelper mongoDBHelper;

    @Before
    public void before() {
        mongoDBHelper = new MongoDBHelper("192.168.3.202", 27017, "test");
    }

    @Test
    public void base() {
        mongoDBHelper.showListCollectionNames();
//        mongoDBHelper.find()
    }

    @Test
    public void findAll() {
        FindIterable<Document> userSource = mongoDBHelper.queryAll("person");
        mongoDBHelper.printResult(userSource);
    }

    @Test
    public void find() {
//        BsonBean bsonBean = new BsonBean(Rule.REGEX, "name", "万人");
//        ArrayList<BsonBean> bsonBeen = new ArrayList<>();
//        bsonBeen.add(bsonBean);
//        FindIterable<Document> userSource = mongoDBHelper.find("person", bsonBean);
        BsonBean bsonBean1 = new BsonBean(Rule.GT, "_id", new ObjectId("58c0ee2e74f24a6d60d3c432"));
//        BsonBean bsonBean3 = new BsonBean(Rule.EQ, "name", "test1");
//        BsonBean bsonBean2 = new BsonBean(Rule.REGEX, "name", "test");
//        BsonBean bsonBean2 = new BsonBean(Rule.LT, "age", 18);
        FindIterable<Document> userSource = mongoDBHelper.query("person", bsonBean1);
//        FindIterable<Document> userSource = mongoDBHelper.query("person", bsonBean1, bsonBean2);
        mongoDBHelper.printResult(userSource);
    }

    @Test
    public void findPage() {
        PagingBean pagingBean = new PagingBean(1, 33);
        pagingBean.setSort(new Sort("age", Order.ASC));
//        Sort sort = new Sort("age", Order.ASC);
//        sort.on("name", Order.ASC);
//        pagingBean.setSort(sort);
//        FindIterable<Document> person = mongoDBHelper.pagingQuery("person", pagingBean,new BsonBean(Rule.NE,"name","x"));
        FindIterable<Document> person = mongoDBHelper.pagingQuerySuper("person", pagingBean);
        mongoDBHelper.printResult(person);
    }

    @Test
    public void findPageSuper() {
        FindIterable<Document> person = mongoDBHelper.pagingQuerySuper("person", new PagingBean(1, 30));
        mongoDBHelper.printResult(person);

//        for (int i = 1; i < 5; i++) {
//            PagingBean pagingBean = new PagingBean(i, 5);
////        pagingBean.setSort(new Sort("age", Order.ASC));
////        Sort sort = new Sort("age", Order.ASC);
////        sort.on("name", Order.ASC);
////        pagingBean.setSort(sort);
////        FindIterable<Document> person = mongoDBHelper.pagingQuery("person", pagingBean,new BsonBean(Rule.NE,"name","x"));
//            FindIterable<Document> person = mongoDBHelper.pagingQuerySuper("person", pagingBean);
//            mongoDBHelper.printResult(person);
//        }
    }

    @Test
    public void delete() {
//        BsonBean bsonBean = new BsonBean(Rule.REGEX, "name", "万人");
//        ArrayList<BsonBean> bsonBeen = new ArrayList<>();
//        bsonBeen.add(bsonBean);
        BsonBean bsonBean1 = new BsonBean(Rule.EQ, "name", "zcq");
        BsonBean bsonBean2 = new BsonBean(Rule.REGEX, "name", "test");
        DeleteResult person = mongoDBHelper.delete("person", bsonBean1, bsonBean2);
        System.out.println(person);
    }

    @Test
    public void insert() {
        HashMap<String, Object> map = new HashMap<>();
//        map.put("_id", 1);
        map.put("name", "zcq");
        map.put("age", 18);
        map.put("birthday", new Date());

        mongoDBHelper.insert("person", map);
    }

    @Test
    public void update() {
        HashMap<String, Object> map = new HashMap<>();
        map.put("name", "千万人");
        map.put("age", "20");
        BsonBean bsonBean = new BsonBean(Rule.EQ, "name", "自行车");
        System.out.println(mongoDBHelper.update("person", map, bsonBean));
    }

    @After
    public void after() {
        mongoDBHelper.close();
    }

}
