package com.netease.arctic.ams.server.optimize;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;

public class LiteBaseFileScanTask implements FileScanTask {

  private final DataFile file;
  private final List<DeleteFile> deletes;
  private final Expression residual;
  private final transient PartitionSpec spec;

  public LiteBaseFileScanTask(FileScanTask scanTask) {
    this.file = scanTask.file().copyWithoutStats();
    this.deletes = scanTask.deletes().stream()
        .map(ContentFile::copyWithoutStats)
        .collect(Collectors.toList());
    this.residual = scanTask.residual();
    this.spec = scanTask.spec();
  }

  @Override
  public DataFile file() {
    return file;
  }

  @Override
  public List<DeleteFile> deletes() {
    return deletes;
  }

  @Override
  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public long start() {
    return 0;
  }

  @Override
  public long length() {
    return file.fileSizeInBytes();
  }

  @Override
  public Expression residual() {
    return residual;
  }

  @Override
  public Iterable<FileScanTask> split(long splitSize) {
    throw new UnsupportedOperationException("Split light file scan is not supported");
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("file", file.path())
        .add("partition_data", file.partition())
        .add("residual", residual())
        .toString();
  }
}

