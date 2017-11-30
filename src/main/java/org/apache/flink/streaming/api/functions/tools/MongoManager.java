package org.apache.flink.streaming.api.functions.tools;



import com.alibaba.fastjson.JSONObject;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;

import java.util.Arrays;

/**
 * Created by admin on 2017/7/31.
 *
 * @author 国庆
 */
public class MongoManager {

    private static MongoManager instance = null;
    private MongoClient mongoClient = null;

    private static final String ID = "_id";

    public static synchronized MongoManager getInstance() {
        return instance = instance == null ? new MongoManager() : instance;
    }

    public MongoClient getClient() {
        return getInstance().mongoClient;
    }

    private MongoManager() {
        try {
            MongoClientOptions.Builder mcob = MongoClientOptions.builder();
            mcob.connectionsPerHost(1000);
            mcob.socketKeepAlive(true);
            mcob.readPreference(ReadPreference.secondaryPreferred());
            mcob.threadsAllowedToBlockForConnectionMultiplier(100);
            MongoClientOptions mco = mcob.build();
        /*mongoClient = new MongoClient(GlobleConfig.getMongoDBs()
                , mco);*/
            mongoClient = new MongoClient(Arrays.asList(
                    new ServerAddress("10.11.0.193", 27017),
                    new ServerAddress("10.11.0.194", 27017),
                    new ServerAddress("10.11.0.195", 27017)), mco);
        } finally {
            //TODO 要不要做点儿什么
            //什么也不做
        }
    }


    public JSONObject executeCount(String dbName, String collectionName, JSONObject reqJson) {

        MongoDatabase db = mongoClient.getDatabase(dbName);
        MongoCollection coll = db.getCollection(collectionName);

        JSONObject resp = new JSONObject();
        Document doc = Document.parse(reqJson.toString());
        long count = coll.count(doc);
        resp.put("Data", count);
        return resp;
    }

    /**
     * @param dbName         表名
     * @param collectionName 集合名
     * @param saveJson       待存入JSON
     * @return 插入状态或异常信息
     */
    @SuppressWarnings("unchecked")
    public JSONObject executeInsert(String dbName, String collectionName, JSONObject saveJson) {
        JSONObject resp = new JSONObject();
        try {
            MongoDatabase db = mongoClient.getDatabase(dbName);
            MongoCollection coll = db.getCollection(collectionName);
            Document doc = Document.parse(saveJson.toString());
            coll.insertOne(doc);
        } catch (MongoTimeoutException e1) {
            e1.printStackTrace();
            resp.put("ReasonMessage", e1.getClass() + ":" + e1.getMessage());
            return resp;
        } catch (Exception e) {
            e.printStackTrace();
            resp.put("ReasonMessage", e.getClass() + ":" + e.getMessage());
        }
        return resp;
    }

    /**
     * @param dbName         库名
     * @param collectionName 表名
     * @param json1          条件
     * @param json2          保存doc
     * @return 更新结果信息
     */
    public JSONObject executeUpdate(String dbName, String collectionName, JSONObject json1, JSONObject json2) {
        JSONObject resp = new JSONObject();
        try {
            MongoDatabase db = mongoClient.getDatabase(dbName);
            MongoCollection coll = db.getCollection(collectionName);
            JSONObject saveJson = new JSONObject();
            saveJson.put("$set", json2);
            Document doc1 = Document.parse(json1.toString());
            Document doc2 = Document.parse(saveJson.toString());
            UpdateResult ur = coll.updateOne(doc1, doc2);
            System.out.println("doc1 = " + doc1.toString());
            System.out.println("doc2 = " + doc2.toString());
            if (ur.isModifiedCountAvailable()) {
                if (json1.containsKey(ID)) {
                    resp.put("Data", json1.get(ID));
                } else {
                    resp.put("Data", json1);
                }
            }
        } catch (MongoTimeoutException e1) {
            e1.printStackTrace();
            resp.put("ReasonMessage", e1.getClass() + ":" + e1.getMessage());
            return resp;
        } catch (Exception e) {
            e.printStackTrace();
            resp.put("ReasonMessage", e.getClass() + ":" + e.getMessage());
        }
        return resp;
    }

    /**
     * @param dbName         库名
     * @param collectionName 集合名
     * @param json1          条件
     * @param json2          保存doc
     * @return upsert结果信息
     */
    public JSONObject executeUpsert(String dbName, String collectionName, JSONObject json1, JSONObject json2) {
        JSONObject resp = new JSONObject();
        try {
            MongoDatabase db = mongoClient.getDatabase(dbName);
            MongoCollection coll = db.getCollection(collectionName);
            JSONObject saveJson = new JSONObject();
            saveJson.put("$set", json2);
            Document doc1 = Document.parse(json1.toString());
            Document doc2 = Document.parse(saveJson.toString());
            UpdateOptions up = new UpdateOptions();
            up.upsert(true);
            UpdateResult ur = coll.updateOne(doc1, doc2, up);
            if (ur.isModifiedCountAvailable()) {
                if (json1.containsKey(ID)) {
                    resp.put("Data", json1.get(ID));
                } else {
                    resp.put("Data", json1);
                }
            }
        } catch (MongoTimeoutException e1) {
            e1.printStackTrace();
            resp.put("ReasonMessage", e1.getClass() + ":" + e1.getMessage());
            return resp;
        } catch (Exception e) {
            e.printStackTrace();
            resp.put("ReasonMessage", e.getClass() + ":" + e.getMessage());
        }
        return resp;
    }
}
