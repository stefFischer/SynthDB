package at.sfischer.synth.db.model;

import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

import java.util.List;

public class Column {

    private final Table table;

    private final ColumnDefinition columnDefinition;

    private Column reference;

    private boolean isAutoIncrement;

    private boolean isPrimaryKey;

    private boolean isUnique;

    public Column(Table table, ColumnDefinition columnDefinition) {
        this.table = table;
        this.columnDefinition = columnDefinition;
        processColumnDefinition();
    }

    private void processColumnDefinition(){
        List<String> specs = this.columnDefinition.getColumnSpecs();
        if(specs == null){
            return;
        }

        for (int i = 0; i < specs.size(); i++) {
            String spec = specs.get(i);
            if (spec.equalsIgnoreCase("AUTO_INCREMENT")) {
                isAutoIncrement = true;
                continue;
            }
            if (spec.equalsIgnoreCase("PRIMARY") && i + 1 < specs.size() && specs.get(i + 1).equalsIgnoreCase("KEY")) {
                isPrimaryKey = true;
                i++;
                continue;
            }
            if (spec.equalsIgnoreCase("UNIQUE")) {
                isUnique = true;
            }
        }
    }

    protected ColumnDefinition getColumnDefinition() {
        return columnDefinition;
    }

    public Table getTable() {
        return table;
    }

    public String getName(){
        return this.columnDefinition.getColumnName();
    }

    public String getType(){
        return this.columnDefinition.getColDataType().getDataType();
    }

    public boolean isAutoIncrement() {
        return isAutoIncrement;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public Column getReference() {
        return reference;
    }

    protected void setReference(Column reference) {
        this.reference = reference;
    }
}
