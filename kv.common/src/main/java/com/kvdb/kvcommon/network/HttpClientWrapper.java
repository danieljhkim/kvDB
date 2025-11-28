package com.kvdb.kvcommon.network;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.*;
import java.time.Duration;

/**
 * HttpClientWrapper client = new HttpClientWrapper();
 *
 * <p>Map<String, String> payload = Map.of("key", "foo", "value", "bar");
 * client.post("http://localhost:9001/set", payload);
 *
 * <p>String value = client.get("http://localhost:9001/get?key=foo", String.class);
 * System.out.println("Value: " + value);
 */
public class HttpClientWrapper {

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public HttpClientWrapper() {
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
        this.objectMapper = new ObjectMapper();
    }

    public <T> T get(String url, Class<T> responseType) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .GET()
                        .timeout(Duration.ofSeconds(3))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        validateResponse(response);
        return objectMapper.readValue(response.body(), responseType);
    }

    public <T> T get(String url, TypeReference<T> responseType) throws Exception {
        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .GET()
                        .timeout(Duration.ofSeconds(3))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        validateResponse(response);
        return objectMapper.readValue(response.body(), responseType);
    }

    public <T> T post(String url, Object requestBody, Class<T> responseType) throws Exception {
        String json = objectMapper.writeValueAsString(requestBody);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        validateResponse(response);
        return objectMapper.readValue(response.body(), responseType);
    }

    public void post(String url, Object requestBody) throws Exception {
        String json = objectMapper.writeValueAsString(requestBody);

        HttpRequest request =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

        HttpResponse<String> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        validateResponse(response);
    }

    private void validateResponse(HttpResponse<?> response) throws Exception {
        int code = response.statusCode();
        if (code >= 200 && code < 300) return;
        throw new RuntimeException(
                "Request failed with status code: " + code + ", body: " + response.body());
    }
}
