package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.Table;

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
}
