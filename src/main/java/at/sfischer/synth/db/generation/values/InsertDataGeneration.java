package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.Table;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Interface for generating SQL INSERT statements for a given table.
 * <p>
 * Implementations provide logic for creating insert statements, potentially using
 * example values from the table itself and values from dependent tables.
 * </p>
 */
public interface InsertDataGeneration {

    /**
     * Generates an SQL INSERT statement for the specified table.
     *
     * @param table the {@link Table} to generate the INSERT statement for
     * @param rowCount the current number of rows already present in the table
     * @param exampleValues a list of example row values from the table to guide generation
     * @param dependentTableValues a map of dependent {@link Table}s to their example values,
     *                             used to satisfy foreign key or other constraints
     * @return a SQL INSERT statement as a {@link String}
     */
    String generateInsertStatement(Table table, long rowCount, List<Map<Column, Object>> exampleValues, Map<Table, List<Map<Column, Object>>> dependentTableValues);

    @NotNull
    static String generateUserMessage(Table table, long rowCount, String values, String otherTableValues) {
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

    static String generateTableValues(List<Map<Column, Object>> rows) {
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

    static String generateDependentTableValues(Map<Table, List<Map<Column, Object>>> dependentTableValues) {
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
