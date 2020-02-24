package com.atguigu.writer;

import com.atguigu.writer.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

public class EsWriterByLuck {
    public static void main(String[] args) throws IOException {
        //新建esCLIENT对象
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder("http://hadoop102:9200").build()
        );
        JestClient esClient = jestClientFactory.getObject();

        Stu stu1 = new Stu();
        stu1.setName("zhangsan");
        stu1.setStu_id(2000);

        Stu stu2 = new Stu();
        stu2.setName("lisi");
        stu2.setStu_id(3000);
        //new Index
        Index build1 = new Index.Builder(stu1)
                .index("student").type("_doc").id("2001").build();

        Index build2 = new Index.Builder(stu2)
                .index("student").type("_doc").id("3001").build();


        Bulk bulk = new Bulk.Builder()
                .addAction(build1)
                .addAction(build2)
                .build();

        esClient.execute(bulk);
        esClient.shutdownClient();

    }
}
