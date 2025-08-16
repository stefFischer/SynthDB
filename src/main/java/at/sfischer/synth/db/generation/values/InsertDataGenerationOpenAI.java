package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.Table;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import com.openai.models.chat.completions.ChatCompletion;
import com.openai.models.chat.completions.ChatCompletionCreateParams;
import com.openai.models.chat.completions.ChatCompletionMessageToolCall;
import com.openai.models.chat.completions.ChatCompletionToolChoiceOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class InsertDataGenerationOpenAI implements InsertDataGeneration {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertDataGenerationOpenAI.class);

    private final String model;

    private final OpenAIClient client;

    public InsertDataGenerationOpenAI(String url, String apiKey, String model) {
        this.model = model;
        this.client = OpenAIOkHttpClient.builder()
                .apiKey(apiKey)
                .baseUrl(url)
                .build();
    }


    public static class InsertRowFunction {
        @JsonProperty("query")
        private String query;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }
    }

    @Override
    public String generateInsertStatement(Table table,
                                          long rowCount,
                                          List<Map<Column, Object>> exampleValues,
                                          Map<Table, List<Map<Column, Object>>> dependentTableValues) {

        String systemPrompt = """
        You are an assistant to generate realistic row of data for the given table in form of a single SQL INSERT statement including the generated single row of data.
        Please try to generate fitting original data not too simple placeholder.
        Return the INSERT inside the `query` field only.
        """;

        String values = InsertDataGeneration.generateTableValues(exampleValues);
        String otherTableValues = InsertDataGeneration.generateDependentTableValues(dependentTableValues);

        String userMessage = InsertDataGeneration.generateUserMessage(table, rowCount, values, otherTableValues);

        try {
            ChatCompletionCreateParams.Builder builder = ChatCompletionCreateParams.builder()
                    .model(model)
                    .addSystemMessage(systemPrompt)
                    .addUserMessage(userMessage)
                    .addTool(InsertRowFunction.class)
                    .toolChoice(ChatCompletionToolChoiceOption.Auto.REQUIRED);

            ChatCompletion completion = client.chat().completions().create(builder.build());
            for (ChatCompletion.Choice choice : completion.choices()) {
                if (choice.message().toolCalls().isPresent()) {
                    for (ChatCompletionMessageToolCall toolCall : choice.message().toolCalls().get()) {
                        if (toolCall.function().name().equals("InsertRowFunction")) {
                            InsertRowFunction parsed = toolCall.function().arguments(InsertRowFunction.class);
                            return parsed.getQuery();
                        }
                    }
                }
            }

            return "";
        } catch (Exception e) {
            LOGGER.warn("Insert statement generation failed.", e);
            return "";
        }
    }
}
