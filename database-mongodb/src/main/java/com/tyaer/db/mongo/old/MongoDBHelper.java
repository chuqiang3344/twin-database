package com.tyaer.db.mongo.old;

import com.mongodb.*;
import org.bson.types.BasicBSONList;
import org.bson.types.ObjectId;

import java.util.*;
import java.util.Map.Entry;

/**
 * Created by Twin on 2017/3/8.
 */
public class MongoDBHelper {

    private static final String HOST = "127.0.0.1";

    private static final String dbName = "test";

    private static Mongo mongo;

    private static DB db;

    static {
        try {
            mongo = new Mongo(HOST);
            db = mongo.getDB(dbName);
            // db.authenticate(username, passwd)
        } catch (MongoException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断集合是否存在
     * <br>------------------------------<br>
     *
     * @param collectionName
     * @return
     */
    public static boolean collectionExists(String collectionName) {
        return db.collectionExists(collectionName);
    }

    /**
     * 查询单个,按主键查询
     * <br>------------------------------<br>
     *
     * @param id
     * @param collectionName
     */
    public static void findById(String id, String collectionName) {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("_id", new ObjectId(id));
        findOne(map, collectionName);
    }

    /**
     * 查询单个
     * <br>------------------------------<br>
     *
     * @param map
     * @param collectionName
     */
    public static void findOne(Map<String, Object> map, String collectionName) {
        DBObject dbObject = getMapped(map);
        DBObject object = getCollection(collectionName).findOne(dbObject);
        print(object);
    }

    /**
     * 查询全部
     * <br>------------------------------<br>
     *
     * @param cursor
     * @param collectionName
     */
    public static void findAll(CursorObject cursor, String collectionName) {
        find(new HashMap<String, Object>(), cursor, collectionName);
    }

    /**
     * count
     * <br>------------------------------<br>
     *
     * @param map
     * @param collectionName
     * @return
     */
    public static long count(Map<String, Object> map, String collectionName) {
        DBObject dbObject = getMapped(map);
        return getCollection(collectionName).count(dbObject);

    }

    /**
     * 按条件查询 </br>
     * 支持skip，limit,sort
     * <br>------------------------------<br>
     *
     * @param map
     * @param cursor
     * @param collectionName
     */
    public static void find(Map<String, Object> map, CursorObject cursor, String collectionName) {
        DBObject dbObject = getMapped(map);
        find(dbObject, cursor, collectionName);
    }

    /**
     * 查询
     * <br>------------------------------<br>
     *
     * @param dbObject
     * @param cursor
     * @param collectionName
     */
    public static void find(DBObject dbObject, final CursorObject cursor, String collectionName) {
        CursorPreparer cursorPreparer = cursor == null ? null : new CursorPreparer() {
            public DBCursor prepare(DBCursor dbCursor) {
                if (cursor == null) {
                    return dbCursor;
                }
                if (cursor.getLimit() <= 0 && cursor.getSkip() <= 0 && cursor.getSortObject() == null) {
                    return dbCursor;
                }
                DBCursor cursorToUse = dbCursor;
                if (cursor.getSkip() > 0) {
                    cursorToUse = cursorToUse.skip(cursor.getSkip());
                }
                if (cursor.getLimit() > 0) {
                    cursorToUse = cursorToUse.limit(cursor.getLimit());
                }
                if (cursor.getSortObject() != null) {
                    cursorToUse = cursorToUse.sort(cursor.getSortObject());
                }
                return cursorToUse;
            }
        };
        find(dbObject, cursor, cursorPreparer, collectionName);
    }

    /**
     * 查询
     * <br>------------------------------<br>
     *
     * @param dbObject
     * @param cursor
     * @param cursorPreparer
     * @param collectionName
     */
    public static void find(DBObject dbObject, CursorObject cursor, CursorPreparer cursorPreparer, String collectionName) {
        DBCursor dbCursor = getCollection(collectionName).find(dbObject);
        if (cursorPreparer != null) {
            dbCursor = cursorPreparer.prepare(dbCursor);
        }
        Iterator<DBObject> iterator = dbCursor.iterator();
        while (iterator.hasNext()) {
            print(iterator.next());
        }
    }

    /**
     * 获取集合(表)
     * <br>------------------------------<br>
     *
     * @param collectionName
     * @return
     */
    public static DBCollection getCollection(String collectionName) {
        return db.getCollection(collectionName);
    }

    /**
     * 获取所有集合名称
     * <br>------------------------------<br>
     *
     * @return
     */
    public static Set<String> getCollection() {
        return db.getCollectionNames();
    }

    /**
     * 创建集合
     * <br>------------------------------<br>
     *
     * @param collectionName
     * @param options
     */
    public static void createCollection(String collectionName, DBObject options) {
        db.createCollection(collectionName, options);
    }

    /**
     * 删除
     * <br>------------------------------<br>
     *
     * @param collectionName
     */
    public static void dropCollection(String collectionName) {
        DBCollection collection = getCollection(collectionName);
        collection.drop();
    }

    /**
     * <br>------------------------------<br>
     *
     * @param map
     * @return
     */
    private static DBObject getMapped(Map<String, Object> map) {
        DBObject dbObject = new BasicDBObject();
        Iterator<Entry<String, Object>> iterable = map.entrySet().iterator();
        while (iterable.hasNext()) {
            Entry<String, Object> entry = iterable.next();
            Object value = entry.getValue();
            String key = entry.getKey();
            if (key.startsWith("$") && value instanceof Map) {
                BasicBSONList basicBSONList = new BasicBSONList();
                Map<String, Object> conditionsMap = ((Map) value);
                Set<String> keys = conditionsMap.keySet();
                for (String k : keys) {
                    Object conditionsValue = conditionsMap.get(k);
                    if (conditionsValue instanceof Collection) {
                        conditionsValue = convertArray(conditionsValue);
                    }
                    DBObject dbObject2 = new BasicDBObject(k, conditionsValue);
                    basicBSONList.add(dbObject2);
                }
                value = basicBSONList;
            } else if (value instanceof Collection) {
                value = convertArray(value);
            } else if (!key.startsWith("$") && value instanceof Map) {
                value = getMapped(((Map) value));
            }
            dbObject.put(key, value);
        }
        return dbObject;
    }

    private static Object[] convertArray(Object value) {
        Object[] values = ((Collection) value).toArray();
        return values;
    }

    private static void print(DBObject object) {
        Set<String> keySet = object.keySet();
        for (String key : keySet) {
            print(object.get(key));
        }
    }

    private static void print(Object object) {
        System.out.println(object.toString());
    }

}

