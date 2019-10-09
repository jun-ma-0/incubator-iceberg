package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.Entry;
import com.adobe.platform.iceberg.extensions.tombstone.Namespace;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AppendFiles;

public interface ExtendedAppendFiles extends AppendFiles {

  /**
   * Append tombstone {@link Entry} to the snapshot.
   *
   * @param namespace a namespace
   * @param entries tombstone values
   * @param properties tombstone properties
   * @return this for method chaining
   */
  ExtendedAppendFiles appendTombstones(Namespace namespace, List<Entry> entries, Map<String, String> properties);

  /**
   * Removes tombstone {@link Entry} to the snapshot.
   *
   * @param namespace a namespace
   * @param entries tombstone values
   * @return this for method chaining
   */
  ExtendedAppendFiles removeTombstones(Namespace namespace, List<Entry> entries);
}
