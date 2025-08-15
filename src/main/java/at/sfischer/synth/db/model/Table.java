package at.sfischer.synth.db.model;

import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.create.table.ForeignKeyIndex;
import net.sf.jsqlparser.statement.create.table.Index;

import java.util.*;

public class Table {

    private final CreateTable createTableStatement;

    private final LinkedHashMap<String, Column> columns;

    public Table(CreateTable createTableStatement) {
        this.createTableStatement = createTableStatement;
        this.columns = new LinkedHashMap<>();
        createTableStatement.getColumnDefinitions().forEach(col -> {
            this.columns.put(col.getColumnName(), new Column(this, col));
        });
    }

    public String getCreateTableStatement(){
        return this.createTableStatement.toString();
    }

    public String getSchemaName(){
        net.sf.jsqlparser.schema.Table tableName = createTableStatement.getTable();
        return tableName.getSchemaName();
    }

    public String getName(){
        return this.createTableStatement.getTable().getName();
    }

    public List<Column> getColumns() {
        return new LinkedList<>(columns.values());
    }

    public Column getColumn(String name) {
        return columns.get(name);
    }

    public void resolveReferences(LinkedHashMap<String, Table> tables) {
        List<Index> indices = createTableStatement.getIndexes();
        if(indices !=null){
            indices.forEach(idx ->{
                if (idx instanceof ForeignKeyIndex fkIdx) {
                    List<String> colNames = idx.getColumnsNames();
                    String tabName = fkIdx.getTable().getName();
                    List<String> refColNames = fkIdx.getReferencedColumnNames();
                    Table referecedTable = tables.get(tabName);
                    for (int i = 0; i < colNames.size(); i++) {
                        Column col = this.columns.get(colNames.get(i));
                        Column refCol = referecedTable.columns.get(refColNames.get(i));
                        col.setReference(refCol);
                    }
                }
            });
        }
        for (Column column : getColumns()) {
            List<String> specs = column.getColumnDefinition().getColumnSpecs();
            if (specs == null) {
                continue;
            }

            for (int i = 0; i < specs.size(); i++) {
                if ("REFERENCES".equalsIgnoreCase(specs.get(i))) {
                    String tabName = specs.get(i + 1);

                    String referencedColumn = null;
                    if (i + 2 < specs.size() && specs.get(i + 2).startsWith("(") && specs.get(i + 2).endsWith(")")) {
                        referencedColumn = specs.get(i + 2).substring(1, specs.get(i + 2).length() - 1);
                    }

                    Table referecedTable = tables.get(tabName);
                    Column refCol = referecedTable.columns.get(referencedColumn);
                    column.setReference(refCol);
                }
            }
        }
    }

    public String generateSelectAll() {
        String columnList = String.join(", ", columns.keySet());
        return "SELECT " + columnList + " FROM " + getName() + ";";
    }

    public String generateSelectRandom(int limit) {
        String columnList = String.join(", ", columns.keySet());
        return "SELECT " + columnList + " FROM " + getName() +
               " ORDER BY RANDOM() LIMIT " + limit + ";";
    }

    public String generateCountSelect() {
        return "SELECT COUNT(*) FROM " + getName();
    }
}
