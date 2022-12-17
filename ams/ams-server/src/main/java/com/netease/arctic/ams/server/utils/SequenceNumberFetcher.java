/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netease.arctic.ams.server.utils;

import com.netease.arctic.IcebergFileEntry;
import com.netease.arctic.scan.TableEntriesScan;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

import java.util.Map;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

/**
 * Utils to get the sequence number of iceberg file.
 * If iceberg table is v1 format, will always return 0.
 */
public class SequenceNumberFetcher implements Closeable {
  private final Table table;
  private final long snapshotId;
  private volatile HTreeMap<String, Long> cached;
  private DB db;

  public static SequenceNumberFetcher with(Table table, long snapshotId) {
    return new SequenceNumberFetcher(table, snapshotId);
  }

  private DB connectDB() {
    if (null != this.db && !db.isClosed()) {
      return db;
    } else {
      return DBMaker.tempFileDB()
          .fileMmapEnableIfSupported()
          .closeOnJvmShutdown()
          .fileLockDisable()
          .transactionEnable()
          .make();
    }
  }

  public SequenceNumberFetcher(Table table, long snapshotId) {
    this.table = table;
    this.snapshotId = snapshotId;
    db = connectDB();
  }

  /**
   * Get Sequence Number of file
   *
   * @param filePath path of a file
   * @return sequenceNumber of this file
   */
  public long sequenceNumberOf(String filePath) {
    Long sequence = getCached().get(filePath);
    Preconditions.checkNotNull(sequence, "can't find sequence of " + filePath);
    return sequence;
  }

  private Map<String, Long> getCached() {
    if (cached == null) {
      cached = connectDB().hashMap("fileSeqNumberMap-" + snapshotId, Serializer.STRING, Serializer.LONG)
          .expireAfterGet()
          .createOrOpen();
      TableEntriesScan manifestReader = TableEntriesScan.builder(table)
          .withAliveEntry(true)
          .includeFileContent(FileContent.DATA, FileContent.POSITION_DELETES, FileContent.EQUALITY_DELETES)
          .useSnapshot(snapshotId)
          .build();
      try (CloseableIterable<IcebergFileEntry> entries = manifestReader.entries()) {
        entries.forEach(e -> cached.put(e.getFile().path().toString(), e.getSequenceNumber()));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close metadata table scan of " + table.name(), e);
      }
      connectDB().commit();
    } else if (cached.isClosed()) {
      cached = connectDB().hashMap("fileSeqNumberMap-" + snapshotId, Serializer.STRING, Serializer.LONG)
          .expireAfterGet().open();
    }
    return cached;
  }

  @Override
  public void close() throws IOException {
    cached.clear();
    db.close();
  }
}
