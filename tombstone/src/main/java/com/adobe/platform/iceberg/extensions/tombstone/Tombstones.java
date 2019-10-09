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
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
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
    SeekableInput input = new AvroFSInput(FileContext.getFileContext(configuration), new Path(fileName));
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
