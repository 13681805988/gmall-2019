package com.atguigu.writer.reader;

import com.alibaba.fastjson.JSONObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.Aggregation;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ESReader {
    public static void main(String[] args) throws IOException {
        //设置参数以及新建EScLIENT
        JestClientFactory jestClientFactory = new JestClientFactory();
        jestClientFactory.setHttpClientConfig(
                new HttpClientConfig.Builder(
                        "http://hadoop102:9200"
                ).build()
        );
        JestClient esClient = jestClientFactory.getObject();

        Search search = new Search.Builder(
                "{\n" +
                        "  \"query\": {\n" +
                        "    \"bool\": {\n" +
                        "      \"filter\": {\n" +
                        "        \"term\": {\n" +
                        "          \"sex\": \"male\"\n" +
                        "        }\n" +
                        "      },\n" +
                        "      \"must\": [\n" +
                        "        {\"match\": {\n" +
                        "          \"favo\": \"球\"\n" +
                        "        }\n" +
                        "        }\n" +
                        "      ]\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"aggs\": {\n" +
                        "    \"count_by_class\": {\n" +
                        "      \"terms\": {\n" +
                        "        \"field\": \"class_id\",\n" +
                        "        \"size\": 2\n" +
                        "      }\n" +
                        "    },\n" +
                        "    \"max_age\":{\n" +
                        "      \"max\": {\n" +
                        "        \"field\": \"age\"\n" +
                        "      }\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"from\": 0,\n" +
                        "  \"size\": 22\n" +
                        "}"
        ).build();

        //获取最终的result
        SearchResult searchResult = esClient.execute(search);

        //解析result
        System.out.println("共查询出符合条件的"+searchResult.getTotal()+"条");
        System.out.println("匹配度最高的是"+searchResult.getMaxScore());

        //获取hits里面的数据
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);

        //遍历hits 获取每一个hit
        for (SearchResult.Hit<Map, Void> hit : hits) {
            JSONObject jsonObject = new JSONObject();
            //获取source
            Map source = hit.source;
            //source是一个map集合 遍历source
            for (Object key : source.keySet()) {
                jsonObject.put(key.toString(),source.get(key));
            }

            //获取source外 hit里面的数据
            jsonObject.put("index",hit.index);
            jsonObject.put("type",hit.type);
            jsonObject.put("id",hit.id);
            System.out.println("hits里的数据为"+jsonObject.toString());
        }



        //获取aggregations
        MetricAggregation aggregations = searchResult.getAggregations();
        MaxAggregation max_age = aggregations.getMaxAggregation("max_age");
        System.out.println("本次查询的最大年龄为:"+max_age.getMax()+"岁");

        //
        TermsAggregation count_by_class = aggregations.getTermsAggregation("count_by_class");
        //遍历buckets
        for (TermsAggregation.Entry entry : count_by_class.getBuckets()) {
            System.out.println(entry.getKey()+"->"+entry.getCount());
        }
    }
}
