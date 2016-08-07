/*
 *  * Copyright 2016 Skymind, Inc.
 *  *
 *  *    Licensed under the Apache License, Version 2.0 (the "License");
 *  *    you may not use this file except in compliance with the License.
 *  *    You may obtain a copy of the License at
 *  *
 *  *        http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *    Unless required by applicable law or agreed to in writing, software
 *  *    distributed under the License is distributed on an "AS IS" BASIS,
 *  *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *    See the License for the specific language governing permissions and
 *  *    limitations under the License.
 */
package org.datavec.flink.functions.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.datavec.api.records.reader.RecordReader;
import org.datavec.api.writable.Writable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.net.URI;
import java.util.List;

/**
 * RecordReaderBytesMapper: Converts binary data (in the form of a BytesWritable) to DataVec format data
 * ({@code Collection<Writable>}) using a RecordReader
 * Created by smarthi
 */
public class RecordReaderBytesMapper implements MapFunction<Tuple2<Text, BytesWritable>, List<Writable>> {

  private final RecordReader recordReader;

  public RecordReaderBytesMapper(RecordReader recordReader) {
    this.recordReader = recordReader;
  }

  @Override
  public List<Writable> map(Tuple2<Text, BytesWritable> tuple2) throws Exception {
    URI uri = new URI(tuple2.f0.toString());
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(tuple2.f1.getBytes()));
    return recordReader.record(uri, dis);
  }
}
