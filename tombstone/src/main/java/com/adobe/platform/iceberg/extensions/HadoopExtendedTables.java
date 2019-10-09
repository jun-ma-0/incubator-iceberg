package com.adobe.platform.iceberg.extensions;

import com.adobe.platform.iceberg.extensions.tombstone.HadoopTombstoneExtension;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;

public class HadoopExtendedTables extends HadoopTables implements ExtendedTables, Configurable {

  public HadoopExtendedTables(Configuration conf) {
    super(conf);
  }

  @Override
  public ExtendedTable loadWithTombstoneExtension(String location) {
    ExtendedTableOperations ops = new ExtendedTableOperations(new Path(location), getConf());
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: " + location);
    }
    return new ExtendedBaseTable(ops, location, new HadoopTombstoneExtension(getConf(), ops));
  }
}
