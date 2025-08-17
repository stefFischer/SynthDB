package at.sfischer.synth.db;

import at.sfischer.synth.db.generation.values.*;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.InsertStatement;
import at.sfischer.synth.db.model.Table;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.io.FileReader;
import java.io.PrintStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class Main implements Callable<Integer> {

    private static final String OPENAI_API_KEY = "OPENAI_API_KEY";

    @Option(
            names = "--provider",
            description = "LLM provider to use. Options: ${COMPLETION-CANDIDATES}, default: ${DEFAULT-VALUE} (For OPENAI you will need to set environment variable: " + OPENAI_API_KEY + ")"
    )
    private LlmProvider provider = LlmProvider.OLLAMA;

    @Option(
            names = "--url",
            description = "Optional URL to LLM, default depends on provider. (OLLAMA: http://localhost:11434/api/chat, OPENAI: https://api.openai.com/v1)"
    )
    private String url;

    @Option(
            names = "--model",
            description = "Optional model used for data generation, default depends on provider. (OLLAMA: llama3.1, OPENAI: gpt-4o-mini)"
    )
    private String model;

    @Option(names = "--examples-per-table", description = "Optional number of value for prompt context, default: ${DEFAULT-VALUE}")
    private Integer examplesPerTable = 2;

    @Option(names = "--schema", description = "Path to schema file in from of SQL CREATE TABLE statements", required = true)
    private Path schemaFilePath;

    @Option(names = "--example-data-file", description = "Optional example data file path of insert statements. Example data can help generating better further data.")
    private Path exampleDataFilePath;

    @Option(names = "--target", description = "Optional target file path for outputs")
    private Path targetFilePath;

    @Option(names = "--database", description = "Database type: ${COMPLETION-CANDIDATES}",
            defaultValue = "MySQL")
    private DatabaseType databaseType = DatabaseType.MySQL;

    @Option(names = "--target-row-number", description = "Optional target row number for all tables, default: ${DEFAULT-VALUE}")
    private Integer targetRowNumber = 5;

    @Option(names = "--target-row-numbers-file", description = "File path specifying target rows per table")
    private Path targetRowNumbersFilePath;

    @Option(names = "--verbose", description = "Enable debug logging")
    private boolean verbose = false;

    private Map<String, Integer> tableTargetRowNumbers = null;

    public enum DatabaseType{
        MySQL,
        PostgreSQL,
//        Oracle, // Problem with SELECT * FROM ... ORDER BY RANDOM() LIMIT 5;
//        MSSQLServer // Problem with SELECT * FROM ... ORDER BY RANDOM() LIMIT 5;
    }

    public enum LlmProvider {
        OLLAMA,
        OPENAI
    }

    @Override
    public Integer call() throws Exception {
        if (verbose) {
           Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
           rootLogger.setLevel(Level.DEBUG);
        }

        if (provider == LlmProvider.OLLAMA) {
            if (url == null) {
                url = "http://localhost:11434/api/chat";
            }
            if (model == null) {
                model = "llama3.1";
            }
        } else if (provider == LlmProvider.OPENAI) {
            if (url == null) {
                url = "https://api.openai.com/v1";
            }
            if (model == null) {
                model = "gpt-4o-mini";
            }
        }

        if (targetRowNumbersFilePath != null) {
            Properties props = new Properties();
            try (Reader reader = Files.newBufferedReader(targetRowNumbersFilePath)) {
                props.load(reader);
            }
            tableTargetRowNumbers = props.entrySet().stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey().toString(),
                            e -> Integer.parseInt(e.getValue().toString())
                    ));
        }

        fillTables();

        return 0;
    }

    public static void main(String[] args){
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    public void fillTables() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:test;MODE=" + this.databaseType.name())) {
            // 1. Set up schema in in-memory database.
            FileReader reader = new FileReader(String.valueOf(this.schemaFilePath));
            DBSchema schema = DBSchema.parseSchema(reader);
            reader.close();
            TableFiller.createSchema(schema, conn);

            // 2. Fill tables with example data if given.
            if (this.exampleDataFilePath != null) {
                reader = new FileReader(String.valueOf(this.exampleDataFilePath));
                List<InsertStatement> exampleDataStatements = InsertStatement.parseInsertStatements(schema, reader);
                reader.close();
                TableFiller.insertData(schema, exampleDataStatements, conn);
            }

            // 3. Generate insert statements.
            InsertDataGeneration insertDataGeneration;
            if(provider == LlmProvider.OPENAI){
                String apiKey = System.getenv(OPENAI_API_KEY);
                if (apiKey == null || apiKey.isBlank()) {
                    throw new IllegalStateException("Missing OpenAI API key. Please set environment variable " + OPENAI_API_KEY);
                }
                insertDataGeneration = new InsertDataGenerationOpenAI(this.url, apiKey, this.model);
            } else {
                insertDataGeneration = new InsertDataGenerationOllama(this.url, this.model);
            }
            Map<Table, List<InsertStatement>> insertStatements;

            TableFillerProgressListener listener = new TableFillerProgressListener() {
                @Override
                public void onProgress(Table table, long rowsGenerated, long totalRows, long tablesCompleted, long totalTables) {
                    System.out.printf(
                            "\r%d/%d rows generated | Table progress: %d/%d | Current Table %-20s",
                            rowsGenerated,
                            totalRows,
                            tablesCompleted,
                            totalTables,
                            table.getName()
                    );
                    System.out.flush();
                }
            };
            if(tableTargetRowNumbers != null){
                insertStatements = TableFiller.fillSchema(schema, conn, insertDataGeneration, tableTargetRowNumbers, examplesPerTable, listener);
            } else {
                insertStatements = TableFiller.fillSchema(schema, conn, insertDataGeneration, targetRowNumber, examplesPerTable, listener);
            }

            // 4. Print results.
            System.out.println("\n----------------------\n");
            PrintStream out;
            if(this.targetFilePath != null){
                out = new PrintStream(String.valueOf(this.targetFilePath));
            } else {
                out = System.out;
            }

            insertStatements.forEach((table, inserts) -> {
                if(inserts == null || inserts.isEmpty()){
                    return;
                }

                try {
                    InsertStatement merged = InsertStatement.mergeStatements(inserts);

                    out.println("-- ==========================");
                    out.println("-- Table data: " + table.getName());
                    out.println("-- ==========================");
                    out.println(merged.generateInsertStatement());
                    out.println();
                } catch (IllegalArgumentException e) {
                    out.println("-- ==========================");
                    out.println("-- Table data: " + table.getName());
                    out.println("-- ==========================");
                    inserts.forEach(insert -> out.println(insert.generateInsertStatement()));
                    out.println();
                }
            });

            out.flush();
            if(this.targetFilePath != null){
                System.out.println("Data stored in: " + this.targetFilePath);
            }
        }
    }

}
