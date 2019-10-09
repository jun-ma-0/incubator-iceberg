package org.apache.iceberg.spark.source;

import com.adobe.platform.iceberg.extensions.ExtendedTable;
import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.TombstoneExtension;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;

import static java.lang.String.format;

public class ExtendedWriter extends Writer {

  private ExtendedTable table;
  private List<Entry> tombstones;
  private Types.NestedField tombstonesField;

  public ExtendedWriter(
      ExtendedTable table,
      DataSourceOptions options,
      boolean replacePartitions,
      String applicationId, String wapId, Schema dsSchema) {
    super(table, options, replacePartitions, applicationId, wapId, dsSchema);
    this.table = table;
    this.tombstones = resolveTombstonesFromOptions(table, options);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    if (tombstones.isEmpty()) {
      super.commit(messages);
    } else {
      appendWithTombstones(messages, tombstones, tombstonesField);
    }
  }

  private void appendWithTombstones(WriterCommitMessage[] messages, List<Entry> entries, Types.NestedField field) {
    AppendFiles append = table.newAppendWithTombstonesAdd(field, entries, Collections.emptyMap());

    int numFiles = 0;
    for (DataFile file : files(messages)) {
      numFiles += 1;
      append.appendFile(file);
    }

    commitOperation(append, numFiles, "appendWithTombstones");
  }

  private List<Entry> resolveTombstonesFromOptions(ExtendedTable table, DataSourceOptions options) {
    Optional<String> tombstoneColumn = options.get(TombstoneExtension.TOMBSTONE_COLUMN);
    if (tombstoneColumn.isPresent()) {
      String column = tombstoneColumn.get();
      tombstonesField = table.schema().findField(column);
      if (tombstonesField == null) {
        throw new RuntimeIOException(format("Failed to find tombstone field by name: %s", column));
      }
      Optional<String> tombstoneValues = options.get(TombstoneExtension.TOMBSTONE_COLUMN_VALUES_LIST);
      if (tombstoneValues.isPresent()) {
        String values = tombstoneValues.get();
        return Stream.of(values.split(",")).map(value -> (Entry) () -> value)
            .collect(ImmutableList.toImmutableList());
      }
    }
    return Collections.emptyList();
  }
}
