package com.atguigu.writer;


import com.atguigu.writer.bean.Stu;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

public class ESwriter {
    public static void main(String[] args) throws IOException {
        //new EsClient
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder
                        ("http://hadoop102:9200").build()
        );
        JestClient esClient = jestClientFactory.getObject();

        //new Stu
        Stu stu = new Stu();
        stu.setName("许伟伟");
        stu.setStu_id(1001);

        Index index = new Index.Builder(stu)
                .index("student")
                .type("_doc")
                .id("1010")
                .build();

        esClient.execute(index);

        esClient.shutdownClient();

    }
}
