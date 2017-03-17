package com.tyaer.database.mongo;

import com.mongodb.*;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tyaer.database.mongo.bean.BsonBean;
import com.tyaer.database.mongo.bean.Order;
import com.tyaer.database.mongo.bean.Rule;
import com.tyaer.database.mongo.cursor.PagingBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

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
        //连接方式1
        mongoClient = new MongoClient(new ServerAddress(host, port), getConfOptions());
        //连接方式2
//        MongoClientURI uri = new MongoClientURI("mongodb://192.168.146.129:27017/mydb", MongoClientOptions.builder().cursorFinalizerEnabled(false));
//        mongoClient = new MongoClient(uri);
        //连接到某个数据库
        database = mongoClient.getDatabase(databaseName);

        // 大部分用户使用mongodb都在安全内网下，但如果将mongodb设为安全验证模式，就需要在客户端提供用户名和密码：
//         boolean auth = database.authenticate(myUserName, myPassword);
    }

    private MongoClientOptions getConfOptions() {
        MongoClientOptions mongoClientOptions = new MongoClientOptions.Builder()
                .socketKeepAlive(true) // 是否保持长链接
                .connectTimeout(15000) // 链接超时时间,连接超时，推荐>3000毫秒
                .socketTimeout(0) // read数据超时时间,套接字超时时间，0无限制
                .maxWaitTime(1000 * 60 * 2) // 长链接的最大等待时间
                .connectionsPerHost(50) // 每个地址最大请求数,连接池设置为300个连接,默认为100
                .threadsAllowedToBlockForConnectionMultiplier(50) // 一个socket最大的等待请求数，线程队列数，如果连接线程排满了队列就会抛出“Out of semaphores to get db”错误。
                .maxConnectionIdleTime(60000) //空闲
                .readPreference(ReadPreference.primary()) // 最近优先策略
//                .autoConnectRetry(false) // 是否重试机制
//                .maxConnectionLifeTime()
                .cursorFinalizerEnabled(true)//表示当没有手动关闭游标时,是否有一个自动释放游标对象的方法,如果你总是很小心的关闭游标,则可以将其设为false 推荐配置true
                .writeConcern(WriteConcern.NORMAL)
                .build();
        return mongoClientOptions;
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
            MongoCollection<Document> mc = getTable(collectionName);
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
     * 查询所有记录
     *
     * @param collectionName
     * @return
     */
    public FindIterable<Document> queryAll(String collectionName) {
        MongoCollection<Document> mongoCollection = getTable(collectionName);
        FindIterable<Document> documents = mongoCollection.find().sort(new BasicDBObject("_id", 1));
        return documents;
    }

    /**
     * 查询
     *
     * @param collectionName
     * @param bsonBeans
     */
    public FindIterable<Document> query(String collectionName, BsonBean... bsonBeans) {
        return query(collectionName, (List) Arrays.asList(bsonBeans));
    }

    public FindIterable<Document> query(String collectionName, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = getTable(collectionName);
        Bson bson = beanToBson(bsonBeans);
        if (bson == null) {
            return mc.find();
        } else {
            return mc.find(bson);
        }
    }

    /**
     * 分页查询
     */
    public FindIterable<Document> pagingQuery(String collectionName, PagingBean pagingBean, BsonBean... bsonBeans) {
        return pagingQuery(collectionName, pagingBean, Arrays.asList(bsonBeans));
    }

    public FindIterable<Document> pagingQuery(String collectionName, PagingBean pagingBean, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = getTable(collectionName);
        Bson bson = beanToBson(bsonBeans);
        Bson orderBy = new BasicDBObject("_id", 1);
        BasicDBObject sortObject = pagingBean.getSortObject();
        if (sortObject != null) {
            orderBy = sortObject;
        }
        int pageIndex = pagingBean.getPageIndex();
        int pageSize = pagingBean.getPageSize();
        FindIterable<Document> iterable;
        if (bson == null) {
            iterable = mc.find();
        } else {
            iterable = mc.find(bson);
        }
        FindIterable<Document> documents = iterable.sort(orderBy).skip((pageIndex - 1) * pageSize).limit(pageSize);
        return documents;
    }


    /**
     * 大数据分页查询，防止内存溢出，根据_id循环查找。
     * 默认按_id正序排序，如果有多种排序，会有问题，最好不设置查询排序。
     *
     * @param collectionName
     * @param pagingBean
     * @param bsonBeans
     * @return
     */
    public FindIterable<Document> pagingQuerySuper(String collectionName, PagingBean pagingBean, BsonBean... bsonBeans) {
        return pagingQuerySuper(collectionName, pagingBean, Arrays.asList(bsonBeans));
    }

    public FindIterable<Document> pagingQuerySuper(String collectionName, PagingBean pagingBean, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = getTable(collectionName);
        ArrayList<Bson> bsonArrayList = beansToBsons(bsonBeans);
        String rowKey = "_id";
        Bson orderBy = new BasicDBObject(rowKey, Order.ASC.getnCode());
        BasicDBObject sortObject = pagingBean.getSortObject();
        if (sortObject != null) {
            orderBy = sortObject.append(rowKey, Order.ASC.getnCode());
        }
        int pageIndex = pagingBean.getPageIndex();
        int pageSize = pagingBean.getPageSize();

        int n = 0;
        ObjectId _id = null;
        FindIterable<Document> documents = null;
        while (n < pageIndex) {
            if (n == 0) {
                Bson bson = null;
                if (bsonArrayList != null) {
                    bson = Filters.and(bsonArrayList);
                }
                FindIterable<Document> iterable;
                if (bson == null) {
                    iterable = mc.find();
                } else {
                    iterable = mc.find(bson);
                }
                documents = iterable.sort(orderBy).limit(pageSize);
                MongoCursor<Document> iterator = documents.iterator();
                while (iterator.hasNext()) {
                    Document document = iterator.next();
                    _id = new ObjectId(document.get(rowKey).toString());
                }
            } else {
                Bson bson = Filters.gt(rowKey, _id);
//                Bson bson = new BasicDBObject(rowKey,new BasicDBObject("$gt",_id));//另一种写法
                if (bsonArrayList != null) {
                    bsonArrayList.add(Filters.gt(rowKey, _id));
                    bson = Filters.and(bsonArrayList);
                }
                documents = mc.find(bson).sort(orderBy).limit(pageSize);
                MongoCursor<Document> iterator = documents.iterator();
                while (iterator.hasNext()) {
                    Document document = iterator.next();
                    _id = new ObjectId(document.get(rowKey).toString());
                }
            }
            n++;
        }
        return documents;
    }

    /**
     * 删除记录
     *
     * @param collectionName
     * @param bsonBeans
     * @return
     */
    public DeleteResult delete(String collectionName, BsonBean... bsonBeans) {
        return delete(collectionName, Arrays.asList(bsonBeans));
    }

    public DeleteResult delete(String collectionName, List<BsonBean> bsonBeans) {
        MongoCollection<Document> mc = getTable(collectionName);
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
        MongoCollection<Document> mc = getTable(collectionName);
        Bson filter;
        if (bsonBeans != null && bsonBeans.size() != 0) {
            filter = beanToBson(bsonBeans);
        } else {
            logger.error("更新条件不能为空！");
            return null;
        }
        Bson document = new Document("$set", map2Document(parameter));
        UpdateOptions options = new UpdateOptions().upsert(true);
        UpdateResult updateResult = mc.updateOne(filter, document, options);// replaceOne(filter, newdoc) 完全替代
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
    private Bson beanToBson(List<BsonBean> bsonBeans) {
        ArrayList<Bson> bsons = beansToBsons(bsonBeans);
        if (bsons != null) {
            return Filters.and(bsons);
        } else {
            return null;
        }
    }

    /**
     * 将自定义的bsonBeans转换为Bson
     *
     * @param bsonBeans
     * @return
     */
    private ArrayList<Bson> beansToBsons(List<BsonBean> bsonBeans) {
        if (bsonBeans != null && bsonBeans.size() > 0) {
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
                    case LT:
                        bson = Filters.lt(bsonBean.getKey(), bsonBean.getValue());
                        break;
                    case GT:
                        bson = Filters.gt(bsonBean.getKey(), bsonBean.getValue());
                        break;
                    case IN:
                        bson = Filters.in(bsonBean.getKey(), bsonBean.getValue());
                        break;
                    case NE:
                        bson = Filters.ne(bsonBean.getKey(), bsonBean.getValue());
                        break;
                    default:
                        logger.error("参数错误：" + rule);
                }
                if (bson != null) {
                    bsonArrayList.add(bson);
                }
            }
            return bsonArrayList;
        } else {
            return null;
        }
    }

    // ------------------------------------共用方法---------------------------------------------------

    /**
     * 获取DB实例 - 指定DB
     *
     * @param dbName
     * @return
     */
    public MongoDatabase getDB(String dbName) {
        if (dbName != null && !"".equals(dbName)) {
            MongoDatabase database = mongoClient.getDatabase(dbName);
            return database;
        }
        return null;
    }

    /**
     * 获取collection对象 - 指定Collection
     *
     * @param collectionName
     * @return
     */
    public MongoCollection<Document> getTable(String collectionName) {
        if (StringUtils.isEmpty(collectionName)) {
            return null;
        }
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection;
    }

    /**
     * 打印查询的结果集
     *
     * @param iterable
     */
    public void printResult(FindIterable<Document> iterable) {
        System.out.println("------------------------------------------------------");
        int n = 0;
        if (iterable != null) {
            for (Document document : iterable) {
                System.out.println(document);
                n++;
            }
        }
        System.out.println("共计：" + n);
        System.out.println("------------------------------------------------------");
    }

    /**
     * 创建单字段索引
     */
    public void createIndex(String collectionName) {
        MongoCollection<Document> mc = getTable(collectionName);
        mc.createIndex(new Document("name", 1));
    }

    /**
     * 集合列表
     */
    public List<String> showListCollectionNames() {
        MongoIterable<String> colls = database.listCollectionNames();
        ArrayList<String> list = new ArrayList<>();
        for (String coll : colls) {
//            System.out.println("CollectionName=" + coll);
            list.add(coll);
        }
        return list;
    }

    /**
     * 删除表
     *
     * @param collectionName
     */
    public void dropCollection(String collectionName) {
        database.getCollection(collectionName).drop();
    }

    /**
     * 删除表
     *
     * @param dbName
     * @param collName
     */
    public void dropCollection(String dbName, String collName) {
        getDB(dbName).getCollection(collName).drop();
    }

    /**
     * 获取所有数据库名称列表
     *
     * @return
     */
    public MongoIterable<String> getAllDBNames() {
        MongoIterable<String> mongoIterable = mongoClient.listDatabaseNames();
        return mongoIterable;
    }

    /**
     * 删除一个数据库
     */
    public void dropDB(String dbName) {
        getDB(dbName).drop();
    }

    /**
     * 查找对象 - 根据主键_id
     *
     * @param coll
     * @param id
     * @return
     */
    public Document findById(MongoCollection<Document> coll, String id) {
        ObjectId _idobj = null;
        try {
            _idobj = new ObjectId(id);
        } catch (Exception e) {
            return null;
        }
        Document myDoc = coll.find(Filters.eq("_id", _idobj)).first();
        return myDoc;
    }

    /**
     * 统计数
     */
    public long getCount(MongoCollection<Document> coll) {
        long count = coll.count();
        return count;
    }

    /**
     * 通过ID删除
     *
     * @param coll
     * @param id
     * @return
     */
    public int deleteById(MongoCollection<Document> coll, String id) {
        int count = 0;
        ObjectId _id = null;
        try {
            _id = new ObjectId(id);
        } catch (Exception e) {
            return 0;
        }
        Bson filter = Filters.eq("_id", _id);
        DeleteResult deleteResult = coll.deleteOne(filter);
        count = (int) deleteResult.getDeletedCount();
        return count;
    }

    /**
     * 通过id更新
     *
     * @param coll
     * @param id
     * @param newdoc
     * @return
     */
    public Document updateById(MongoCollection<Document> coll, String id, Document newdoc) {
        ObjectId _idobj = null;
        try {
            _idobj = new ObjectId(id);
        } catch (Exception e) {
            return null;
        }
        Bson filter = Filters.eq("_id", _idobj);
        // coll.replaceOne(filter, newdoc); // 完全替代
        coll.updateOne(filter, new Document("$set", newdoc));
        return newdoc;
    }


    /**
     * 断开连接
     */
    public void close() {
        if (mongoClient != null) {
            logger.info("关闭连接...");
            mongoClient.close();
            mongoClient = null;
        }
    }

}
