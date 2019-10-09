Tombstone - column oriented data restatement with snapshot isolation guarantees 

### Semantics

When tombstones are applied to an Iceberg table data files can be described as any of the following
mutually exclusive groups 

Group | Notation | Semantic
------------- | ------------- | -------------
Mixed | M | Has at least one tombstone row but also other valid rows 
Strict tombstone | T | Has only tombstone rows
Strict valid | !T | Has zero tombstone rows

* When reading data using the extended table API or the extended Spark reader the underlying 
expression must target only M and !T files, since targeting T files is redundant due to the 
tombstone filtering that would reduce to zero rows of these files.

* When loading data for the tombstone reduce process using the extended table API or the 
extended Spark reader the underlying expression must target only M and T files, since targeting !T 
files are redundant due to the tombstone filtering that would not affect the rows of these files.

> A considerable difference from the naive read operation is that the output of the tombstone 
reduce reader is meant to be replacing the original files.

### Iceberg API extension

Examples of the Tombstone API extensions    

#### Append files and add tombstones
```java
import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.HadoopExtendedTables;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.AppendFiles;

public final HadoopExtendedTables tables = new HadoopExtendedTables(new Configuration());
ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
Types.NestedField field = table.schema().findField("batch");
table
  .newAppendWithTombstonesAdd(
    field,
    Lists.newArrayList(() -> "1", () -> "2", () -> "3"),
    ImmutableMap.of("purgeByMillis", "1571226183000", "reason", "test"))
  .appendFile(...)
  .commit();
```

#### Append files and remove tombstones

```java
import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.HadoopExtendedTables;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.AppendFiles;

public final HadoopExtendedTables tables = new HadoopExtendedTables(new Configuration());
ExtendedTable table = tables.loadWithTombstoneExtension(getTableLocation());
Types.NestedField field = table.schema().findField("batch");
table
  .newAppendWithTombstonesRemove(
    field,
    Lists.newArrayList(() -> "1", () -> "2", () -> "3"))
  .appendFile(...)
  .commit();
```

### Spark reader/ writer extensions 

Examples of using the Iceberg Spark extended reader/writer to accommodate
tombstone extension use-cases

#### Write (append) files and add tombstone for column

```java
public static SparkSession spark;

spark.createDataFrame([rows], [schema])
    .write()
    .format("iceberg.adobe")
    .option("iceberg.extension.tombstone.col", "_acp_system_metadata.acp_sourceBatchId")
    .option("iceberg.extension.tombstone.values", "a,b,c")
    .mode(SaveMode.Append)
    .save(getTableLocation())
``` 

#### Read (filter tombstone rows)

```java
public static SparkSession spark;

spark
    .read()
    .format("iceberg.adobe")
    .option("iceberg.extension.tombstone.col", "_acp_system_metadata.acp_sourceBatchId")
    .load(getTableLocation())
    .count()
```

#### Tombstone reduce

When using the `iceberg.extension.tombstone.vacuum` option an underlying Iceberg expression
will identify files that have at least one partition rows, reduce from the resulted dataframe
all the tombstone rows and then overwrite the original files that where used to hydrate the 
dataframe with new files generated from the remaining dataframe. 

```java
import com.adobe.platform.iceberg.extensions.ExtendedTable;

ExtendedTable table = TABLES.loadWithTombstoneExtension(getTableLocation());
long snapshotId = table.currentSnapshot().snapshotId();

spark.read()
    .format("iceberg.adobe")
    .option("iceberg.extension.tombstone.vacuum", "")
    .option("iceberg.extension.tombstone.col", "data")
    .option("snapshot-id", snapshotId)
    .load(getTableLocation())
    .write()
    .format("iceberg.adobe")
    .mode(SaveMode.Overwrite)
    .option("iceberg.extension.tombstone.vacuum", "")
    .option("iceberg.extension.tombstone.col", "data")
    .option("snapshot-id", snapshotId)
    .save(getTableLocation());
```

In the case of subsequent commits of rows containing tombstone valued columns  
 the implementation will throw an `org.apache.iceberg.exceptions.ValidationException` 
 wrapped in an `org.apache.spark.SparkException.class` 
