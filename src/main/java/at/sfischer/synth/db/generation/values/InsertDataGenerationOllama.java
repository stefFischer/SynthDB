package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.generation.ollama.OllamaStructuredHelper;
import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.Table;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpTimeoutException;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link InsertDataGeneration} that generates SQL INSERT statements
 * using the Ollama AI service.
 */
public class InsertDataGenerationOllama implements InsertDataGeneration {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertDataGenerationOllama.class);

    private final String url;
    private final String model;

    /**
     * Constructs a new InsertDataGenerationOllama instance.
     *
     * @param url   the endpoint URL of the Ollama AI service
     * @param model the AI model name to use for generating insert statements
     */
    public InsertDataGenerationOllama(String url, String model) {
        this.url = url;
        this.model = model;
    }

    /**
     * Generates an SQL INSERT statement for the specified table using the Ollama AI model.
     *
     * @param table the {@link Table} to generate the INSERT statement for
     * @param rowCount the current number of rows already present in the table
     * @param exampleValues a list of example row values from the table
     * @param dependentTableValues a map of dependent {@link Table}s to their example values
     * @return a SQL INSERT statement as a {@link String}
     */
    @Override
    public String generateInsertStatement(Table table, long rowCount, List<Map<Column, Object>> exampleValues, Map<Table, List<Map<Column, Object>>> dependentTableValues) {

        String systemPrompt = """
        You are an assistant to generate realistic row of data for the given table in form of a single SQL INSERT statement including the generated single row of data.
        Please try to generate fitting original data not too simple placeholder.
        """;

        String values = InsertDataGeneration.generateTableValues(exampleValues);
        String otherTableValues = InsertDataGeneration.generateDependentTableValues(dependentTableValues);

        String userMessage = InsertDataGeneration.generateUserMessage(table, rowCount, values, otherTableValues);

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
}
