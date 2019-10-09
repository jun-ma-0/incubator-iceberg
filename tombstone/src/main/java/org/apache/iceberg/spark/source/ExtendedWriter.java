package org.apache.iceberg.spark.source;

import static java.lang.String.format;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.Vacuum;
import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.ExtendedEntry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.ImmutableList;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

public class ExtendedWriter extends Writer {

  private ExtendedTable table;
  private List<Entry> tombstones = new ArrayList<>();
  // We preserve the dotted notation field name since org.apache.iceberg.types.Types.NestedField
  // does not provide full precedence of field name so we'd fail any SQL queries
  private Map.Entry<String, Types.NestedField> column;
  private Boolean isVacuum;
  private DataSourceOptions options;

  public ExtendedWriter(
      ExtendedTable table,
      DataSourceOptions options,
      boolean replacePartitions,
      String applicationId, String wapId, Schema dsSchema) {
    super(table, options, replacePartitions, applicationId, wapId, dsSchema);
    this.table = table;
    this.options = options;
    this.isVacuum = options.get(TombstoneExtension.TOMBSTONE_VACUUM).isPresent();
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (isVacuum) {
      column = tombstoneColumn(options);
      long readSnapshotId = options.getLong("snapshot-id", 0L);
      this.tombstones = table
          .getSnapshotTombstones(column.getValue(), table.snapshot(readSnapshotId))
          .stream().map(ExtendedEntry::getEntry).collect(Collectors.toList());
      vacuum(messages, tombstones, column, readSnapshotId);
    } else {
      this.tombstones = tombstoneValues(options);
      if (tombstones.isEmpty()) {
        super.commit(messages);
      } else {
        this.column = tombstoneColumn(options);
        appendWithTombstones(messages, tombstones, column.getValue());
      }
    }
  }

  private Map.Entry<String, Types.NestedField> tombstoneColumn(DataSourceOptions options) {
    Optional<String> tombstoneColumn = options.get(TombstoneExtension.TOMBSTONE_COLUMN);
    if (tombstoneColumn.isPresent()) {
      String columnName = tombstoneColumn.get();
      NestedField column = table.schema().findField(columnName);
      if (column != null) {
        return new SimpleEntry<>(columnName, column);
      }
    }
    throw new RuntimeIOException(
        format("Failed to find tombstone field by name: %s", tombstoneColumn));
  }

  private List<Entry> tombstoneValues(DataSourceOptions options) {
    Optional<String> tombstoneValues = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST);
    if (tombstoneValues.isPresent()) {
      return Stream.of(tombstoneValues.get().split(",")).map(value -> (Entry) () -> value)
          .collect(ImmutableList.toImmutableList());
    }
    return Collections.emptyList();
  }

  private void vacuum(WriterCommitMessage[] messages, List<Entry> entries,
      Map.Entry<String, Types.NestedField> column, Long readSnapshotId) {
    Vacuum vacuum = table.newVacuumTombstones(column, entries, readSnapshotId);

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      vacuum.addFile(file);
    }

    commitOperation(vacuum, numFiles, "vacuumTombstones");
  }

  private void appendWithTombstones(WriterCommitMessage[] messages, List<Entry> entries,
      Types.NestedField field) {
    AppendFiles append = table.newAppendWithTombstonesAdd(field, entries, new HashMap<>());

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    commitOperation(append, numFiles, "appendWithTombstones");
  }
}
