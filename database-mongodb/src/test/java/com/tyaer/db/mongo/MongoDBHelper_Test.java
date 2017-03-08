package com.tyaer.db.mongo;

import com.mongodb.client.FindIterable;
import com.mongodb.client.result.DeleteResult;
import com.tyaer.db.mongo.bean.BsonBean;
import com.tyaer.db.mongo.bean.Rule;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    public void findAll() {
        FindIterable<Document> userSource = mongoDBHelper.findAll("person");
        for (Document document : userSource) {
            System.out.println(document);
        }
    }

    @Test
    public void find() {
//        BsonBean bsonBean = new BsonBean(Rule.REGEX, "name", "万人");
//        ArrayList<BsonBean> bsonBeen = new ArrayList<>();
//        bsonBeen.add(bsonBean);
//        FindIterable<Document> userSource = mongoDBHelper.find("person", bsonBean);
        BsonBean bsonBean1 = new BsonBean(Rule.EQ, "name", "zcq");
//        BsonBean bsonBean2 = new BsonBean(Rule.REGEX, "name", "test");
        BsonBean bsonBean2 = new BsonBean(Rule.REGEX, "age", "18");
        FindIterable<Document> userSource = mongoDBHelper.find("person", bsonBean1, bsonBean2);
        mongoDBHelper.printResult(userSource);
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
        map.put("_id", 1);
        map.put("name", "zcq");
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
