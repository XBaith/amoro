package com.netease.arctic.ams.server.optimize;

import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.expressions.Expression;

public class IcebergBaseFileScanTask implements FileScanTask {

  private final DataFile file;
  private final List<DeleteFile> deletes;
  private final Expression residual;

  private transient PartitionSpec spec = null;

  public IcebergBaseFileScanTask(FileScanTask scanTask) {
    this.file = scanTask.file().copyWithoutStats();
    this.deletes = scanTask.deletes();
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
}
