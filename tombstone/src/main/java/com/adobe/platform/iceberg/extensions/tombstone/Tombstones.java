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

package com.adobe.platform.iceberg.extensions.tombstone;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Tombstones {
  private static final ReflectData REFLECT_DATA = ReflectData.AllowNull.get();
  private Configuration configuration;

  public Tombstones(Configuration configuration) {
    this.configuration = configuration;
  }

  public List<Tombstone> load(String fileName) throws IOException {
    List<Tombstone> tombstones = new ArrayList<>();
    Schema schema = REFLECT_DATA.getSchema(Tombstone.class);
    DatumReader<Tombstone> datumReader = new ReflectDatumReader<>(schema);
    // Note: don't use alternative {@link org.apache.hadoop.fs.AvroFSInput} it causes exceptions w/ DB runtime
    // Issue tracked at https://jira.corp.adobe.com/browse/PLAT-48669
    SeekableInput input = new FsInput(new Path(fileName), configuration);
    try (FileReader<Tombstone> dataFileReader = DataFileReader.openReader(input, datumReader)) {
      while (dataFileReader.hasNext()) {
        tombstones.add(dataFileReader.next(null));
      }
    }
    return tombstones;
  }

  public void write(List<Tombstone> tombstones, String fileName) throws IOException, URISyntaxException {
    Schema schema = REFLECT_DATA.getSchema(Tombstone.class);
    DatumWriter<Tombstone> writer = new ReflectDatumWriter<>(schema);
    FileSystem fs = FileSystem.get(new URI(fileName), this.configuration);
    // Don't create w/ overwrite option, if the file exists this should throw an exception
    FSDataOutputStream out = fs.create(new Path(fileName), false);
    try (DataFileWriter<Tombstone> fileWriter = new DataFileWriter<>(writer).create(schema, out)) {
      for (Tombstone tombstone : tombstones) {
        fileWriter.append(tombstone);
      }
    }
  }
}
