package org.apache.flink.streaming.api.functions.sink;


import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.streaming.api.functions.tools.MongoManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 张建新 on 2017/7/31.
 *
 * @author 张建新
 */
public class MongoSink extends RichSinkFunction<Tuple4<String, Integer, String, Integer>> {
    private static Logger logger = LoggerFactory.getLogger(MongoSink.class);
    private MongoManager mongoManager;

    @Override
    public void invoke(Tuple4<String, Integer, String, Integer> inTuple4) throws InterruptedException {
        try {
            if (inTuple4 != null) {
                if (inTuple4.f2 != null && !"".equals(inTuple4.f2.trim())) {
                    JSONObject data = JSONObject.parseObject(inTuple4.f2);
                    JSONObject filter = data.getJSONObject("filter");
                    JSONObject doc = data.getJSONObject("data");
                    if (data.getString("dbName") != null) {
                        mongoManager.executeUpsert(data.getString("dbName"), data.getString("collectionName"), filter, doc);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Mongo入库发生捕获的异常！{}:{}", e.getClass(), e.getMessage());
            for (StackTraceElement element : e.getStackTrace()) {
                logger.error("\tat {}", element.toString());
            }
        }
    }

    @Override
    public void open(Configuration config) {
        mongoManager = MongoManager.getInstance();
        try {
            super.open(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String s = "{\"filter\":{},\"data\":{\"address\":\"asdasd\",\"lastModifyTime\":asdad,\"phoneNum\":\"******\",\"_id\":\"1404593256\",\"asd\":\"任**\"},\"dbName\":\"dbName\",\"collectionName\":\"collectionName\"}";
        JSONObject data = JSONObject.parseObject(s);
        MongoManager mongoManager = MongoManager.getInstance();
        JSONObject filter = data.getJSONObject("filter");
        JSONObject doc = data.getJSONObject("data");
        mongoManager.executeUpsert(data.getString("dbName"), data.getString("collectionName"), filter, doc);
    }
}