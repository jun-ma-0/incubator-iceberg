package com.adobe.platform.iceberg.extensions;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class WithSpark implements WithDefaultIcebergTable {
  public static SparkSession spark;
  public final HadoopExtendedTables TABLES = new HadoopExtendedTables(new Configuration());

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableDir = null;
  private String tableLocation = null;

  @BeforeClass
  public static void startSpark() {
    WithSpark.spark = SparkSession.builder()
        .master("local[2]")
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    WithSpark.spark.stop();
    WithSpark.spark = null;
  }

  public File getTableDir() {
    return tableDir;
  }

  public String getTableLocation() {
    return tableLocation;
  }

  @Before
  public void setupTable() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // remove this if you want to debug persisted data on disk
    this.tableLocation = tableDir.toURI().toString();
    implicitTable(TABLES, tableLocation);
  }
}
