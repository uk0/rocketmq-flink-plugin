package org.apache.flink.streaming.api.functions.sink;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.tools.MongoManager;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangjianxin on 2017/7/31.
 *
 * @author Brian
 */
public class MongoUpsertSink extends RichSinkFunction<String> {

    public static final String UPDATE_KEY = "uk";
    public static final String TO_UPDATE_JSON = "tuj";
    public static final String DB_NAME = "dn";
    public static final String COLLECTION_NAME = "cn";

    private static Logger logger = LoggerFactory.getLogger(MongoUpsertSink.class);
    private MongoClient mongoClient;
    private static UpdateOptions uo;

    /**
     * @param inJsonStr 传入Json样例
     * @throws InterruptedException
     */
    @Override
    public void invoke(String inJsonStr) throws InterruptedException {
        try {
            if (inJsonStr == null || "".equals(inJsonStr)) {
                return;
            }
            JSONObject json = JSONObject.parseObject(inJsonStr);
            Document toUpdateDoc;
            Document updateKey;
            String dbName;
            String collName;
            try {
                toUpdateDoc = Document.parse(json.getString(TO_UPDATE_JSON));
                updateKey = Document.parse(json.getString(UPDATE_KEY));
                dbName = json.getString(DB_NAME);
                collName = json.getString(COLLECTION_NAME);
            } catch (Exception e) {
                return;
            }
            if (dbName == null || collName == null || "".equals(dbName) || "".equals(collName)) {
                return;
            }
            mongoClient.getDatabase(dbName).getCollection(collName).updateMany(updateKey, toUpdateDoc, uo);
        } catch (Exception e) {
            logger.error("Mongo入库发生未知异常！{}:{}", e.getClass(), e.getMessage());
            for (StackTraceElement element : e.getStackTrace()) {
                logger.error("\tat {}", element.toString());
            }
        }
    }

    @Override
    public void open(Configuration config) {
        mongoClient = MongoManager.getInstance().getClient();
        uo = new UpdateOptions();
        uo.upsert(true);
        try {
            super.open(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
