package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Column;
import at.sfischer.synth.db.model.Table;

import java.util.List;
import java.util.Map;

public interface InsertDataGeneration {
    String generateInsertStatement(Table table, long rowCount, List<Map<Column, Object>> exampleValues, Map<Table, List<Map<Column, Object>>> dependentTableValues);
}
