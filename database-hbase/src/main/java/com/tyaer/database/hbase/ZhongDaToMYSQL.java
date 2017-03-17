package com.tyaer.database.hbase;

import com.tyaer.database.mysql.MySQLHelperPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Twin on 2017/2/17.
 */
public class ZhongDaToMYSQL {
    private static final Log logger = LogFactory.getLog(ZhongDaToMYSQL.class);
    static MySQLHelperPool mysqlHelper;
    static Pager pager;
    static String tableName;
    static String sql = "insert ignore into t_weibo values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

    static {
        mysqlHelper = new MySQLHelperPool("root", "123456", "jdbc:mysql://127.0.0.1:3306/zhongda?useUnicode=true&characterEncoding=UTF-8");
        tableName = "vt_weibo";

    }

    public static void main(String[] args) {
        pager = new Pager(tableName);
        String start;
        String end;
        ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(32);
        for (int i = 0; i <= 65535; i++) {
//            n = String.valueOf(i);
//            System.out.println(n);
            start = Integer.toHexString(i);
//            start = get4(start)+"#";
            start = get4(start);
            end = Integer.toHexString(i + 1);
//            end = get4(end)+"#";
            end = get4(end);
            executorService.execute(new EruptTransfer(start, end));
        }
        while (true) {
            int activeCount = executorService.getActiveCount();
            if (activeCount == 0) {
                logger.info("重新启动完毕！");
                break;
            } else {
//                logger.info("activeCount: " + activeCount);
                logger.info("executorService getQueue: " + executorService.getQueue().size());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static String get4(String start) {
        switch (start.length()) {
            case 1:
                start = "000" + start;
                break;
            case 2:
                start = "00" + start;
                break;
            case 3:
                start = "0" + start;
                break;
            default:
                break;
        }
        return start;
    }

    static class EruptTransfer extends Thread {
        private String start;
        private String end;

        public EruptTransfer(String start, String end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public void run() {
//            logger.info("page find: " + start + " => " + end);
            List<Map<String, String>> mapList = pager.scanByRowRegion(start, end, 0, false);
//            List<Map<String, String>> mapList = pager.scanByRowRegion("0001", "0005", 0, false);
            ArrayList<ArrayList<Object>> arrayLists = new ArrayList<ArrayList<Object>>();
            for (Map<String, String> map : mapList) {
                ArrayList<Object> objects = new ArrayList<Object>();
//                System.out.println(map.get("rowkey"));
                String mid = map.get("mid");
                String uid = map.get("uid");
                String topic = map.get("topic");
                String at_who = map.get("at_who");
                String comments_count = map.get("comments_count");
                String zan_count = map.get("zan_count");
                String reposts_count = map.get("reposts_count");
                String created_at = map.get("created_at");
                String updatetime = map.get("updatetime");
//                String create_time = map.get("create_time");
                String emotion = map.get("emotion");
                String mid_p = map.get("mid_p");
                String name = map.get("name");
                String pic = map.get("pic");
                String profileImageUrl = map.get("profileImageUrl");
                String reposts_depth = map.get("reposts_depth");
                String source = map.get("source");
                String text = map.get("text");
                String text_loc = map.get("text_loc");
                String weibo_url = map.get("weibo_url");

                objects.add(mid);
                objects.add(uid);
                objects.add(topic);
                objects.add(at_who);
                objects.add(comments_count);
                objects.add(zan_count);
                objects.add(reposts_count);
                objects.add(created_at);
                objects.add(updatetime);
                objects.add(new Timestamp(Calendar.getInstance().getTimeInMillis()));
                objects.add(emotion);
                objects.add(mid_p);
                objects.add(name);
                objects.add(pic);
                objects.add(profileImageUrl);
                objects.add(reposts_depth);
                objects.add(source);
                objects.add(text);
                objects.add(text_loc);
                objects.add(weibo_url);
                arrayLists.add(objects);
            }
            int size = mapList.size();
            if (size > 0) {
                logger.info("## " + start + " => " + end + " 当前区域数据量：" + size);
                //插入到mysql
//                mysqlHelper.batchUpdateByPreparedStatement(sql, arrayLists);
                arrayLists.clear();
//                System.out.println(arrayLists);
//                break;
            } else {
                logger.info("当前区域无数据！");
            }
        }
    }


}
