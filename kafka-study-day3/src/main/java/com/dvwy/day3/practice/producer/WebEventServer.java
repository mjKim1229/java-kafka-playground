package com.dvwy.day3.practice.producer;

import fi.iki.elonen.NanoHTTPD;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class WebEventServer extends NanoHTTPD {
    private static final String WEB_ROOT = "./"; // index.html이 있는 디렉토리
    private static EventProducer eventProducer;

    public WebEventServer(int port) {
        super(port);
    }

    public static void main(String[] args) throws Exception {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "[::1]:9092");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        eventProducer = new EventProducer(configs);
        eventProducer.send("key", "value");

        WebEventServer server = new WebEventServer(8080);
        server.start(SOCKET_READ_TIMEOUT, false);
        System.out.println("HTTP-to-Kafka 서버 시작! http://localhost:8080/");
    }

    @Override
    public Response serve(IHTTPSession session) {
        String uri = session.getUri();
        Method method = session.getMethod();

        try {

            // static 파일 제공
            if (uri.equals("/") || uri.endsWith(".html") || uri.endsWith(".js") || uri.endsWith(".css")) {
                return serveStaticFile(uri.equals("/") ? "/index.html" : uri);
            }

            // 이벤트 수신용 API
            if (method.equals(Method.GET) && uri.equals("/push")) {
                session.parseBody(null);
                Map<String, String> params = session.getParms();
                String user = params.get("user");
                String color = params.get("color");

                if (user == null || color == null) {
                    return newFixedLengthResponse(Response.Status.BAD_REQUEST, "application/json", "{\"error\":\"missing parameters\"}");
                }

                JSONObject json = new JSONObject();
                json.put("user", user);
                json.put("color", color);
                eventProducer.send(user, json.toString());

                return newFixedLengthResponse(Response.Status.OK, "application/json", "{\"status\":\"ok\"}");
            }

            return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "404 Not Found");
        } catch (Exception e) {
            e.printStackTrace();
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Internal Server Error");
        }
    }

    private Response serveStaticFile(String path) {
        File file = new File(WEB_ROOT, path);
        if (!file.exists()) {
            return newFixedLengthResponse(Response.Status.NOT_FOUND, MIME_PLAINTEXT, "404 Not Found");
        }
        try {
            FileInputStream fis = new FileInputStream(file);
            String mime = getMimeTypeForFile(path);
            return newChunkedResponse(Response.Status.OK, mime, fis);
        } catch (IOException e) {
            e.printStackTrace();
            return newFixedLengthResponse(Response.Status.INTERNAL_ERROR, MIME_PLAINTEXT, "Failed to load file");
        }
    }
}
