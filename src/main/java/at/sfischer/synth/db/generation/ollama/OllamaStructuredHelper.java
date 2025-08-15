package at.sfischer.synth.db.generation.ollama;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class OllamaStructuredHelper {

    private static final HttpClient client = HttpClient.newHttpClient();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static JsonNode callOllama(
            String url,
            String model,
            String systemPrompt,
            String userMessage,
            JsonNode formatDefinition
    ) throws IOException, InterruptedException {

        String requestBody = String.format("""
        {
          "model": "%s",
          "temperature": 1.2,
          "top_p": 0.9,
          "repeat_penalty": 1.2,
          "stream": false,
          "messages": [
            { "role": "system", "content": %s },
            { "role": "user", "content": %s }
          ],
          "format": %s
        }
        """,
                model,
                mapper.writeValueAsString(systemPrompt),
                mapper.writeValueAsString(userMessage),
                mapper.writeValueAsString(formatDefinition)
        );

        HttpRequest request = HttpRequest.newBuilder()
                .timeout(Duration.ofSeconds(5))
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        JsonNode root = mapper.readTree(response.body());
        return mapper.readTree(root.path("message").path("content").asText());
    }
}
