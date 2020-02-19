/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.adobe.platform.iceberg.extensions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkStrategy;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2StrategyWithAdobeFilteringAndPruning$;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConverters;

public class WithSpark implements WithDefaultIcebergTable {
  public static SparkSession sparkWithTombstonesExtension;
  public static SparkSession spark;
  public final HadoopExtendedTables tables = new HadoopExtendedTables(new Configuration());

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableDir = null;
  private String tableLocation = null;

  @BeforeClass
  public static void startSpark() {
    sparkWithTombstonesExtension = SparkSession.builder()
        .master("local[2]")
        .getOrCreate();

    spark = sparkWithTombstonesExtension.newSession();

    if (sparkWithTombstonesExtension != null) {
      List<SparkStrategy> extra = new ArrayList<>();
      extra.add(DataSourceV2StrategyWithAdobeFilteringAndPruning$.MODULE$);
      sparkWithTombstonesExtension.experimental().extraStrategies_$eq(
          JavaConverters.asScalaBufferConverter(extra).asScala().toSeq());
    }
  }

  @AfterClass
  public static void stopSpark() {
    sparkWithTombstonesExtension.stop();
    sparkWithTombstonesExtension = null;
    spark.stop();
    spark = null;
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
    implicitTable(tables, tableLocation);
  }
}
