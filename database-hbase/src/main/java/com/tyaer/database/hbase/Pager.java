package com.tyaer.database.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;

import java.io.IOException;
import java.util.*;

public class Pager {
    private static final Log logger = LogFactory.getLog(Pager.class);
    public static Configuration configuration;
    private static HTable hTable;
    private static String startRow = null;
    private static List list = null;

    static {
        configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.zookeeper.quorum", "192.168.2.112,192.168.2.113,192.168.2.115");
//        configuration.set("hbase.zookeeper.quorum", "192.168.2.112");
//        configuration.set("hbase.master", "192.168.10.120:60000");
    }

    private String tableName;

    public Pager(String tableName) {

        try {
            this.hTable = new HTable(configuration, tableName.getBytes());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Pager pager = new Pager("vt_weibo");
//        Pager pager = new Pager("t_user");
        pager.getPage(1, 1000);
    }

    public static List getLast(int pageNum, int pageSize) {
        getPage(pageNum - 1, pageSize);
        return null;
    }

    /**
     * 取得下一页 这个类是接着getPage来用
     *
     * @param pageSize 分页的大小
     * @return 返回分页数据
     */
    public static List getNext(int pageSize) throws Exception {
        Filter filter = new PageFilter(pageSize + 1);
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.setStartRow(startRow.getBytes());
        ResultScanner result = hTable.getScanner(scan);
        Iterator iterator = result.iterator();
        list = new ArrayList<Result>();
        int count = 0;
        for (Result r : result) {
            count++;
            if (count == pageSize + 1) {
                startRow = new String(r.getRow());

                scan.setStartRow(startRow.getBytes());
                System.out.println("startRow" + startRow);
                break;
            } else {
                list.add(r);
            }
            startRow = new String(r.getRow());
            System.out.println(startRow);
            //把 r的所有的列都取出来     key-value age-20
            System.out.println(count);
        }
        return list;

    }

    // pageNum = 3 pageSize = 10
    public static void getPage(int pageNum, int pageSize) {
        List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
        // int pageNow = 0;
        // TODO 这个filter到底是干嘛的？
//        int totalSize = pageNum * pageSize;
        Scan scan = new Scan();
        scan.setStartRow("1000000".getBytes());
        scan.setFilter(new PageFilter(pageSize));
//        scan.setReversed(true);//顺序反转的功能
        //pageNum = 3   需要扫描3页
        for (int i = 0; i < pageNum; i++) {
            try {
                ResultScanner rs = hTable.getScanner(scan);
                int count = 0;
                for (Result result : rs) {
                    count++;
//                    if (count == pageSize + 1) {
//                        // TODO: 2017/2/17  一个分片才管用，否则 re.size=pageSize*10;
//                        startRow = new String(result.getRow());
//                        scan.setStartRow(startRow.getBytes());
//                        System.out.println("startRow" + startRow);
//                        break;
//                    }
                    String rowkey = new String(result.getRow());
                    System.out.println("rowkey:" + rowkey);
                    //把 r的所有的列都取出来     key-value age-20
                    List<Cell> cells = result.listCells();
                    Map<String, String> map = new HashMap<String, String>();
                    map.put("rowkey", rowkey);
                    for (Cell cell : cells) {
                        map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
                    }
                    lists.add(map);
                }
                System.out.println(count);
                //查询完毕
                if (count < pageSize) {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static List<Map<String, String>> scanByRowRegion(String start, String end, int topN, boolean desc) {

        List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
        try {
            Scan scan = new Scan(start.getBytes(), end.getBytes());
//            Scan scan = new Scan();
//            scan.setStartRow(start.getBytes());
//            scan.setStopRow(end.getBytes());
            if (topN != 0) {
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filterList.addFilter(new PageFilter(topN));
                scan.setFilter(filterList);
            }
//            scan.setReversed(desc);//顺序反转的功能
//            scan.setMaxVersions(1);

//            logger.info("begin scan");

            ResultScanner results = hTable.getScanner(scan);

//            logger.info("after scan");

            // 输出结果
            for (Result result : results) {
                Map<String, String> map = new HashMap<String, String>();
                for (Cell cell : result.rawCells()) {
                    map.put(new String(CellUtil.cloneQualifier(cell),"utf-8"), new String(CellUtil.cloneValue(cell),"utf-8"));
                }

                map.put("rowkey", new String(result.getRow(),"utf-8"));

                lists.add(map);
            }
//            logger.info("prepare result");
        } catch (IOException e) {
            logger.error("getRowsByCondition has exception={}", e);
        } finally {
//            if (table != null) {
//                try {
//                    table.close();
//                    table = null;
//                } catch (IOException e) {
//                    logger.error("close HTable has exception={}", e);
//                }
//            }
        }
        return lists;

    }
}
