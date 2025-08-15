package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.generation.ollama.OllamaStructuredHelper;
import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InsertDataGenerationOllama implements InsertDataGeneration {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertDataGenerationOllama.class);

    private final String url;
    private final String model;

    public InsertDataGenerationOllama(String url, String model) {
        this.url = url;
        this.model = model;
    }

    @Override
    public String generateInsertStatement(Table table, long rowCount, List<Map<Column, Object>> exampleValues, Map<Table, List<Map<Column, Object>>> dependentTableValues) {

        String systemPrompt = """
        You are an assistant to generate realistic row of data for the given table in form of a single SQL INSERT statement including the generated single row of data.
        Please try to generate fitting original data not too simple placeholder.
        """;

        String values = generateTableValues(exampleValues);
        String otherTableValues = generateDependentTableValues(dependentTableValues);

        String userMessage = generateUserMessage(table, rowCount, values, otherTableValues);

        String format = """
        {
          "type": "object",
          "properties": {
            "query": { "type": "string" }
          },
          "required": ["query"]
        }
        """;

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode formatNode = mapper.readTree(format);

            JsonNode response = OllamaStructuredHelper.callOllama(
                    url,
                    model,
                    systemPrompt,
                    userMessage,
                    formatNode
            );

            return response.path("query").asText();
        } catch (HttpTimeoutException e) {
            LOGGER.debug("Insert statement generation timed out.");
            return "";
        } catch (Exception e) {
            LOGGER.warn("Insert statement generation failed.", e);
            return "";
        }
    }

    @NotNull
    public static String generateUserMessage(Table table, long rowCount, String values, String otherTableValues) {
        String userMessageTemplate = """
        This is the table to generate data for:
        ```
        %s
        ```
        There are already %d rows in the table.
        Here are some example values already in the table:
        %s

        %s
        """;

        return String.format(userMessageTemplate, table.getCreateTableStatement(), rowCount, values, otherTableValues).trim();
    }

    protected static String generateTableValues(List<Map<Column, Object>> rows) {
        if(rows == null || rows.isEmpty()){
            return "";
        }

        StringBuilder sb = new StringBuilder();

        List<Column> columns = new ArrayList<>(rows.getFirst().keySet());
        sb.append("|");
        for (Column col : columns) {
            sb.append(" ").append(col.getName()).append(" |");
        }
        sb.append("\n");

        sb.append("|");
        sb.append(" --- |".repeat(columns.size()));
        sb.append("\n");

        for (Map<Column, Object> row : rows) {
            sb.append("|");
            for (Column col : columns) {
                Object value = row.get(col);
                sb.append(" ").append(value != null ? value.toString() : "").append(" |");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    protected static String generateDependentTableValues(Map<Table, List<Map<Column, Object>>> dependentTableValues) {
        if(dependentTableValues == null){
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (Map.Entry<Table, List<Map<Column, Object>>> tableEntry : dependentTableValues.entrySet()) {
            Table table = tableEntry.getKey();
            List<Map<Column, Object>> rows = tableEntry.getValue();
            if (rows.isEmpty()) {
                continue;
            }

            sb.append("Table: ").append(table.getName()).append("\n");

            sb.append(generateTableValues(rows));

            sb.append("\n");
        }

        return sb.toString();
    }
}
