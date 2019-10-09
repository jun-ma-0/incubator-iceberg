package com.adobe.platform.iceberg.extensions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTableOperations;

/**
 * No overrides happen in this class, we only use it because we can't create instance of {@link HadoopTableOperations}
 */
public class ExtendedTableOperations extends HadoopTableOperations implements TableOperations {

  public ExtendedTableOperations(Path location, Configuration conf) {
    super(location, conf);
  }

  @Override
  public void commit(TableMetadata init, TableMetadata result) {
    super.commit(init, result);
  }
}
