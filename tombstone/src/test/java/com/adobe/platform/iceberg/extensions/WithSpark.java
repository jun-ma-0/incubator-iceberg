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
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class WithSpark implements WithDefaultIcebergTable {
  public static SparkSession spark;
  public final HadoopExtendedTables tables = new HadoopExtendedTables(new Configuration());

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
    implicitTable(tables, tableLocation);
  }
}
