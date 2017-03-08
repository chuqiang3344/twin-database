package com.tyaer.db.mongo;

/**
 * Created by Twin on 2017/3/8.
 */
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Sorts.ascending;

/**
 * Created by peidw on 2016/9/2.
 */
public class MongodbTest {

    @Test
    public void do1(){
        String myUserName = "admin";
        String myPassword = "admin";
        MongoClient mongoClient = new MongoClient("192.168.146.129", 27017);
        Assert.assertNotNull(mongoClient);

        //连接数据库
        MongoDatabase db=mongoClient.getDatabase("mydb");
        mongoClient.setWriteConcern(WriteConcern.JOURNALED);
        //无密码 验证用户
        MongoClientURI uri = new MongoClientURI("mongodb://192.168.146.129:27017/mydb", MongoClientOptions.builder().cursorFinalizerEnabled(false));
        MongoClient client = new MongoClient(uri);
        MongoDatabase mydb=client.getDatabase("mydb");
        MongoCollection<Document> collection = mydb.getCollection("users");
        List<Document> foundDocument = collection.find().into(new ArrayList<Document>());
        System.out.println(foundDocument);
        //集合列表
        // 4.集合列表
        MongoIterable<String> colls =  mydb.listCollectionNames();
        for (String s : colls) {
            System.out.println("CollectionName=" + s);
        }
        //获得某集合对象
        MongoCollection<Document> mc = mydb.getCollection("users");
        mc.drop();
        //把记录插入集合
        Document users_dc1= new Document("name", "killcsdn").append("age",22).append("email","10000@qq.com");
        Document users_dc2= new Document("name", "javaeye").append("age",33).append("email","10001@qq.com");
        mc.insertMany(Arrays.asList(users_dc1,users_dc2));
        //测试: 查询全部
        FindIterable<Document> iterable = mc.find();
        printResult("find all", iterable);

        //创建单字段索引
        mc.createIndex(new Document("name", 1));


        //根据条件查询
        //查询age=22
        iterable = mc.find(new Document("age", 22));
        printResult("age=22", iterable);
        //查询name like %csdn% and owner=tom
        iterable = mc.find( and (regex("name", "j"), eq("age", 33)));
        printResult("find name like %j% and age=33", iterable);

        //查询全部按title排序
        iterable = mc.find().sort(ascending("name"));
        printResult("find all and ascending name", iterable);

        //记录不存在新增，存在更新
        Bson filter = Filters.eq("name", "裴xx");
        Bson xxdoc = new Document("$set", new Document("name", "裴xx").append("age",350).append("email","kkk@qq.com"));
        UpdateOptions options = new UpdateOptions().upsert(true);
        mc.updateOne(filter,xxdoc,options);

        Bson filter2 = Filters.eq("name", "裴xx");
        Bson xxdoc2 = new Document("$set", new Document("name", "裴xx更新后").append("age",350).append("email","kkk@qq.com"));
        UpdateOptions options2 = new UpdateOptions().upsert(true);
        mc.updateOne(filter2,xxdoc2,options2);

        FindIterable<Document> iterable_up = mc.find();
        printResult("find all", iterable_up);

    }
    //打印查询的结果集
    public void printResult(String doing, FindIterable<Document> iterable) {
        System.out.println(doing);
        iterable.forEach(new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("------------------------------------------------------");
        System.out.println();
    }
}
