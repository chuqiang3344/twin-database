package com.tyaer.db.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.text.ParseException;

/**
 * Created by Twin on 2017/3/8.
 */
public class CudExamples {
    public static void main(String[] args) throws ParseException {
        //根据实际环境修改ip和端口
        MongoClient mongoClient = new MongoClient("192.168.3.202", 27017);
        MongoDatabase database = mongoClient.getDatabase("collection");

        MongoCollection<Document> mc = database.getCollection("userSource");
//        mc.drop();
        //插入一个文档
//        mc.insertOne(new Document("oop", "java"));
//        System.out.println(mc.findOneAndDelete(new Document("oop", "java")));
        FindIterable<Document> documents = mc.find();
        for (Document document : documents) {
            System.out.println(document);
        }

        mongoClient.close();
    }
}
