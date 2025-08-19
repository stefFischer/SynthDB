# SynthDB – Synthetic Data Generator for SQL Databases

> Generate realistic, constraint-aware test data for your SQL-style databases.  
> Currently supports **MySQL** and **PostgreSQL**.

---

## Description

SynthDB is a CLI and library tool for generating synthetic, constraint-aware data for SQL-style databases.

It automatically:

- Reads your schema (DDL)
- Understands foreign key relationships
- Fills tables in dependency order
- Uses AI models (Ollama, OpenAI) to create realistic content

---

## Features

- ✅ Schema parsing for MySQL and PostgreSQL
- ✅ Foreign key–aware data generation
- ✅ Configurable target rows per table
- ✅ Example-based AI generation (Ollama structured outputs, OpenAI tool calling)
- ✅ CLI and library modes

---

## Installation

Clone and build with Gradle:

```bash
git clone https://github.com/stefFischer/SynthDB.git
cd synthdb
./gradlew shadowJar -x test
```

### Running Tests

Some tests depend on an [Ollama Docker container](https://hub.docker.com/r/ollama/ollama) with the `llama3.1` model available.
- If you don’t have Ollama installed (or don’t want to run it locally), you can skip the tests by adding -x test when building.
- If you don’t want to set up Ollama manually, the test class `at.sfischer.synth.db.DBFillTests` will automatically, pull the current image, start an Ollama container and pull the required model if it’s not already present.


## Usage

```bash
java -jar synthdb.jar --schema ./schema.sql 
```
There is some example data in [`src/test/resources/examples`](src/test/resources/examples) to try it out.

### Command-line Options

| Option | Description                                                                                                                                                                                                                                              | Default | Required |
|--------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|----------|
| `--schema=<schemaFilePath>` | Path to schema file containing SQL `CREATE TABLE` statements.                                                                                                                                                                                            | – | **Yes** |
| `--provider=<provider>` | LLM provider to use. Options: OLLAMA, OPENAI. (For OPENAI you will need to set environment variable: `OPENAI_API_KEY`)                                                                                                                                   | `OLLAMA` | No |
| `--url=<url>` | URL of the LLM API endpoint.                                                                                                                                                                                                                             | depends on provider | No |
| `--model=<model>` | AI model used for data generation.                                                                                                                                                                                                                       | depends on provider | No |
| `--database=<databaseType>` | Database type to use. Supported: `MySQL`, `PostgreSQL`.                                                                                                                                                                                                  | `MySQL` | No |
| `--example-data-file=<exampleDataFilePath>` | Path to a file containing example `INSERT` statements. Example data can help generate more realistic additional data. `SynthDB` treats these entries as part of the final database. Additional data will be generated around them to ensure consistency. | – | No |
| `--examples-per-table=<examplesPerTable>` | Number of example rows per table to include in the AI prompt context. ATTENTION: Too many examples can lead to halluciations in smaller models (e.g., foreign keys that do not exist).                                                                   | `2` | No |
| `--target=<targetFilePath>` | Path to file where generated output will be written. If not set the output will be written to STDOUT.                                                                                                                                                    | – | No |
| `--target-row-number=<targetRowNumber>` | Target row count for all tables (if not specified per table).                                                                                                                                                                                            | `5` | No |
| `--target-row-numbers-file=<targetRowNumbersFilePath>` | Path to file specifying target row counts per table (properties file).                                                                                                                                                                                   | – | No |
| `--verbose` | Enable debug logging output.                                                                                                                                                                                                                             | Off | No |


## Programmatic Usage Example

In addition to running **SynthDB** from the command line, you can also use it directly in your Java code to create and populate schemas with synthetic data.

### Example: Generating Data for an Employee/Department Schema

```java
import at.sfischer.synth.db.generation.values.InsertDataGeneration;
import at.sfischer.synth.db.generation.values.InsertDataGenerationOllama;
import at.sfischer.synth.db.generation.values.TableFiller;
import at.sfischer.synth.db.model.DBSchema;
import at.sfischer.synth.db.model.InsertStatement;
import at.sfischer.synth.db.model.Table;

...

String URL = "http://localhost:11434/api/chat";
String MODEL = "llama3.1";
InsertDataGeneration INSERT_DATA_GENERATION_OLLAMA = new InsertDataGenerationOllama(URL, MODEL);

java.sql.Connection conn;

// Define schema using SQL DDL. (COMMENTs can help the LLM by explaining the columns or tables)
String ddl = """
    CREATE TABLE employee (
        id INT PRIMARY KEY AUTO_INCREMENT,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        hire_date DATE NOT NULL COMMENT 'Date when the employee was hired',
        department_id INT REFERENCES department(id)
    ) COMMENT='Stores company employees and their employment details';

    CREATE TABLE department (
        id INT PRIMARY KEY AUTO_INCREMENT,
        name VARCHAR(100) NOT NULL,
        location VARCHAR(100)
    );
""";

// Provide example data (optional, helps generate more realistic results)
String exampleData = """
    INSERT INTO employee (first_name, last_name, hire_date, department_id) VALUES
        ('Astrid', 'Kovach', '2009-06-19', 2),
        ('Liam', 'Mendez', '2018-02-01', 1);

    INSERT INTO department (id, name, location) VALUES
        (1, 'Sales', 'New York'),
        (2, 'Marketing', 'New York');
""";

// Set per-table target row numbers
Map<String, Integer> tableTargetRowNumbers = Map.of(
    "employee", 10,
    "department", 5
);

// Parse schema from DDL
DBSchema schema = DBSchema.parseSchema(ddl);

// Create the schema in the target database
TableFiller.createSchema(schema, conn);

// Parse and insert example data
List<InsertStatement> exampleDataStatements =
    InsertStatement.parseInsertStatements(schema, exampleData);
TableFiller.insertData(schema, exampleDataStatements, conn);

// Fill schema with synthetic data
Map<Table, List<InsertStatement>> insertStatements =
    TableFiller.fillSchema(schema, conn, INSERT_DATA_GENERATION_OLLAMA, tableTargetRowNumbers, 5);

// Print generated data
insertStatements.forEach((table, inserts) -> {
    inserts.forEach(statement -> System.out.println(statement.generateInsertStatement()));

    // Optionally merge inserts per table into one statement
    InsertStatement merged = InsertStatement.mergeStatements(inserts);
    System.out.println(merged.generateInsertStatement());
});
```

## Dependencies and Limitations

SynthDB relies on the following libraries and components:

- **[JSQLParser](https://github.com/JSQLParser/JSqlParser)**: Used to parse SQL `CREATE TABLE` and `INSERT` statements.  
  ⚠️ Note: Not all SQL features from all databases are fully supported. Some complex statements might fail to parse.

- **[H2 Database](https://www.h2database.com/)**: Used internally for temporary schema creation and data insertion.  
  ⚠️ Note: H2 may have limitations compared to MySQL or PostgreSQL. Features like certain constraints, data types, or SQL dialect-specific syntax may not behave exactly the same as in the target database.


### Other Limitations

- Only MySQL and PostgreSQL are currently supported.


