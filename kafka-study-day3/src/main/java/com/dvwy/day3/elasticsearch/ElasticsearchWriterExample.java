package com.dvwy.day3.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClient;
import org.apache.http.HttpHost;
import org.json.JSONObject;
import java.util.Map;

public class ElasticsearchWriterExample {
    public static void main(String[] args) throws Exception {
        // Elasticsearch 클라이언트 초기화
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200, "http")
        ).build();

        RestClientTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper()
        );

        ElasticsearchClient client = new ElasticsearchClient(transport);

        // 예제 JSON 문자열
        String jsonString = "{\"color\":\"red\", \"user\":\"dane\"}";
        JSONObject json = new JSONObject(jsonString);
        Map<String, Object> jsonMap = json.toMap();

        // 색인 요청
        IndexRequest<Map<String, Object>> request = IndexRequest.of(i -> i
                .index("color-log")
                .id("uniqueKey") // 여기에 유니크키를 넣어서 중복을 막을 수 있다
                .document(jsonMap)
        );

        IndexResponse response = client.index(request);
        System.out.println("✅ 색인 성공, ID: " + response.id());

        // 리소스 정리
        restClient.close();
    }
}