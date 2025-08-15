package at.sfischer.synth.db.generation.values;

import at.sfischer.synth.db.model.Table;

public abstract class TableFillerProgressListener {

    private Table currentTable;
    private long totalRows;
    private long rowsGenerated;

    private long totalTables;
    private long tablesCompleted;

    /**
     * Called when a table has some progress.
     *
     * @param table The table currently being filled
     * @param rowsGenerated Number of rows generated so far for this table
     * @param totalRows Target number of rows for this table
     * @param tablesCompleted Number of tables fully filled so far
     * @param totalTables Total number of tables to fill
     */
    public abstract void onProgress(Table table, long rowsGenerated, long totalRows, long tablesCompleted, long totalTables);

    public void setTotalTables(long totalTables) {
        this.totalTables = totalTables;
    }

    public long getTotalTables() {
        return totalTables;
    }

    public void nextTable(Table nextTable, long rowsToGenerate){
        this.currentTable = nextTable;
        this.totalRows = rowsToGenerate;
        this.rowsGenerated = 0;
        this.tablesCompleted++;

        onProgress(currentTable, rowsGenerated, totalRows, tablesCompleted, totalTables);
    }

    public void rowGenerated(){
        this.rowsGenerated++;

        onProgress(currentTable, rowsGenerated, totalRows, tablesCompleted, totalTables);
    }
}

