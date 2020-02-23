package com.atguigu.client;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.client.utils.MykafkaSender.MyKafkaSender;
import com.atguigu.constants.GmallConstants;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalCli{
    public static void main(String[] args) throws InvalidProtocolBufferException {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example", "", "");
        //抓取数据流程
        while (true){
            //连接canal
            canalConnector.connect();
            //指定订阅的哪个SQL数据库
            canalConnector.subscribe("gmall.*");
            //获取SQL中的数据 获取到的是多个Entry的集合 每个entry对应一条SQL数据
            Message message = canalConnector.get(100);
            //获取entries集合
            List<CanalEntry.Entry> entries = message.getEntries();
            //判断拉取的批次中是否有数据
            if(entries.size()==0){
                System.out.println("没有变化数据,请休息一会..");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //存在变化数据,遍历entries,取出每一个entry
                for (CanalEntry.Entry entry : entries) {
                    //判断操作类型
                    if(CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())){
                        //取出序列化后的数据 rowChange
                        CanalEntry.RowChange rowChange =
                                CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        //获取表名
                        String tableName = entry.getHeader().getTableName();
                        //获取事件的类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //发送到kafka处理
                        MyHandler(tableName,eventType,rowChange);
                    }
                }
            }


        }


    }

    private static void MyHandler(String tableName, CanalEntry.EventType eventType,
                                  CanalEntry.RowChange rowChange) {
       //判断
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //获取rowdatalist行集合
            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
            //遍历行集合取出每行数据
            for (CanalEntry.RowData rowData : rowDatasList) {
                //创建JSON
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("tableName",tableName);
                List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
                for (CanalEntry.Column column : afterColumnsList) {
                    jsonObject.put(column.getName(),column.getValue());
                }
                //打印
                System.out.println(jsonObject.toString());
                //kafka生产者发送至集群
                MyKafkaSender.send(GmallConstants.GMALL_ORDER_INFO_TOPIC,
                        jsonObject.toString());
            }
        }
    }

}
