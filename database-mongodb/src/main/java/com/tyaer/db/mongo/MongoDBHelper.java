package com.tyaer.db.mongo;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tyaer.db.mongo.bean.BsonBean;
import com.tyaer.db.mongo.bean.Rule;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.*;

/**
 * Created by Twin on 2017/3/8.
 */
public class MongoDBHelper {
    private static final Logger logger = Logger.getLogger(MongoDBHelper.class);

    private String host;
    private int port = 27017;
    private String databaseName;

    private MongoClient mongoClient;
    private MongoDatabase database;

    public MongoDBHelper(String host, int port, String databaseName) {
        this.host = host;
        this.port = port;
        this.databaseName = databaseName;
        init();
    }

    private void init() {
        //根据实际环境修改ip和端口
        mongoClient = new MongoClient(host, port);
        database = mongoClient.getDatabase(databaseName);
    }

    public FindIterable<Document> findAll(String collectionName) {
        MongoCollection<Document> mc = database.getCollection(collectionName);
        FindIterable<Document> documents = mc.find();
        return documents;
    }

    /**
     * 插入数据
     *
     * @param collectionName
     * @param parameter
     */
    public void insert(String collectionName, HashMap<String, Object>... parameter) {
        insert(collectionName, Arrays.asList(parameter));
    }

    public void insert(String collectionName, List<HashMap<String, Object>> parameters) {
        if (parameters != null && parameters.size() != 0) {
            MongoCollection<Document> mc = database.getCollection(collectionName);
            if (parameters.size() == 1) {
                //插入单条条记录
                Document document = map2Document(parameters.get(0));
                mc.insertOne(document);
            } else {
                //插入多条记录
                ArrayList<Document> documents = new ArrayList<>();
                for (HashMap<String, Object> parameter : parameters) {
                    Document document = map2Document(parameter);
                    documents.add(document);
                }
                mc.insertMany(documents);
            }
        } else {
            logger.error("参数不能为空！");
        }
    }

    /**
     * 查询
     *
     * @param collectionName
     * @param bsonBeans
     */
    public FindIterable<Document> find(String collectionName, BsonBean... bsonBeans) {
        return find(collectionName, (List) Arrays.asList(bsonBeans));
    }

    public FindIterable<Document> find(String collectionName, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = database.getCollection(collectionName);
        Bson bson = beanToBson(bsonBeans);
        FindIterable<Document> documents = mc.find(bson);
        return documents;
    }

    /**
     * 删除记录
     * @param collectionName
     * @param bsonBeans
     * @return
     */
    public DeleteResult delete(String collectionName, BsonBean... bsonBeans) {
        return delete(collectionName, Arrays.asList(bsonBeans));
    }

    public DeleteResult delete(String collectionName, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = database.getCollection(collectionName);
        Bson bson = beanToBson(bsonBeans);
        DeleteResult deleteResult = mc.deleteMany(bson);
        return deleteResult;
    }

    /**
     * 更新,记录不存在新增，存在更新
     *
     * @param collectionName
     * @param parameter
     * @param bsonBeans
     * @return
     */
    public UpdateResult update(String collectionName, HashMap<String, Object> parameter, BsonBean... bsonBeans) {
        return update(collectionName, parameter, (List) Arrays.asList(bsonBeans));
    }

    public UpdateResult update(String collectionName, HashMap<String, Object> parameter, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = database.getCollection(collectionName);
        Bson filter;
        if (bsonBeans != null && bsonBeans.size() != 0) {
            filter = beanToBson(bsonBeans);
        } else {
            logger.error("更新条件不能为空！");
            return null;
        }
        Bson document = new Document("$set", map2Document(parameter));
        UpdateOptions options = new UpdateOptions().upsert(true);
        UpdateResult updateResult = mc.updateOne(filter, document, options);
        return updateResult;
    }

    /**
     * 将参数转换为Document
     *
     * @param parameter
     * @return
     */
    public Document map2Document(HashMap<String, Object> parameter) {
        Document document = new Document();
        for (Map.Entry<String, Object> entry : parameter.entrySet()) {
            document.append(entry.getKey(), entry.getValue());
        }
        return document;
    }

    /**
     * 将自定义的bsonBeans转换为Bson
     *
     * @param bsonBeans
     * @return
     */
    private Bson beanToBson(Iterable<BsonBean> bsonBeans) {
        ArrayList<Bson> bsonArrayList = new ArrayList<>();
        for (BsonBean bsonBean : bsonBeans) {
            Rule rule = bsonBean.getRule();
            Bson bson = null;
            switch (rule) {
                case EQ:
                    bson = Filters.eq(bsonBean.getKey(), bsonBean.getValue());
                    break;
                case REGEX:
                    bson = Filters.regex(bsonBean.getKey(), (String) bsonBean.getValue());
                    break;
                default:
                    logger.error("参数错误：" + rule);
            }
            if (bson != null) {
                bsonArrayList.add(bson);
            }
        }
        return Filters.and((bsonArrayList));
    }

    /**
     * 打印查询的结果集
     *
     * @param doing
     * @param iterable
     */
    public void printResult(String doing, FindIterable<Document> iterable) {
        System.out.println(doing);
        printResult(iterable);
    }

    public void printResult(FindIterable<Document> iterable) {
        System.out.println("------------------------------------------------------");
        iterable.forEach(new Block<Document>() {
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        System.out.println("------------------------------------------------------");
    }


    /**
     * 创建单字段索引
     */
    public void createIndex(String collectionName) {
        MongoCollection<Document> mc = database.getCollection(collectionName);
        mc.createIndex(new Document("name", 1));
    }

    /**
     * 集合列表
     */
    public void showListCollectionNames() {
        MongoIterable<String> colls = database.listCollectionNames();
        for (String s : colls) {
            System.out.println("CollectionName=" + s);
        }
    }

    /**
     * 删除表
     *
     * @param collectionName
     */
    public void drop(String collectionName) {
        MongoCollection<Document> mc = database.getCollection(collectionName);
        //每次执行前清空集合以方便重复运行
        mc.drop();
    }

    public void close() {
        logger.info("关闭连接");
        mongoClient.close();
    }

}
