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

package com.netease.arctic.optimizer.operator.executor;

import com.google.common.collect.Iterables;
import com.netease.arctic.ams.api.OptimizeTaskId;
import com.netease.arctic.ams.api.OptimizeType;
import com.netease.arctic.data.DataFileType;
import com.netease.arctic.data.DataTreeNode;
import com.netease.arctic.data.IcebergContentFile;
import com.netease.arctic.table.TableIdentifier;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class NodeTask {
  private static final Logger LOG = LoggerFactory.getLogger(NodeTask.class);
  private final List<ContentFile<?>> allFiles = new ArrayList<>();
  private final List<DataFile> dataFiles = new ArrayList<>();
  private final List<DataFile> baseFiles = new ArrayList<>();
  private final List<DataFile> insertFiles = new ArrayList<>();
  private final List<DataFile> deleteFiles = new ArrayList<>();
  private final List<DeleteFile> posDeleteFiles = new ArrayList<>();
  private final List<IcebergContentFile> icebergDataFiles = new ArrayList<>();
  private final List<IcebergContentFile> icebergSmallDataFiles = new ArrayList<>();
  private final List<IcebergContentFile> icebergEqDeleteFiles = new ArrayList<>();
  private final List<IcebergContentFile> icebergPosDeleteFiles = new ArrayList<>();
  private Set<DataTreeNode> sourceNodes;
  private StructLike partition;
  private OptimizeTaskId taskId;
  private TableIdentifier tableIdentifier;
  private int attemptId;
  private String customHiveSubdirectory;
  private Long maxExecuteTime;

  public NodeTask() {
  }

  public StructLike getPartition() {
    return partition;
  }

  public void setPartition(StructLike partition) {
    this.partition = partition;
  }

  public void addFile(ContentFile<?> file, DataFileType fileType) {
    if (fileType == null) {
      LOG.warn("file type is null");
      return;
    }

    switch (fileType) {
      case BASE_FILE:
        baseFiles.add((DataFile) file);
        break;
      case INSERT_FILE:
        insertFiles.add((DataFile) file);
        break;
      case EQ_DELETE_FILE:
        deleteFiles.add((DataFile) file);
        break;
      case POS_DELETE_FILE:
        posDeleteFiles.add((DeleteFile) file);
        break;
      default:
        LOG.warn("file type is {}, not add in node", fileType);
        // ignore the object
    }
  }

  public void addFile(IcebergContentFile icebergContentFile, DataFileType fileType) {
    if (fileType == null) {
      LOG.warn("file type is null");
      return;
    }

    switch (fileType) {
      case BASE_FILE:
        icebergDataFiles.add(icebergContentFile);
        break;
      case INSERT_FILE:
        icebergSmallDataFiles.add(icebergContentFile);
        break;
      case ICEBERG_EQ_DELETE_FILE:
      case EQ_DELETE_FILE:
        icebergEqDeleteFiles.add(icebergContentFile);
        break;
      case POS_DELETE_FILE:
        icebergPosDeleteFiles.add(icebergContentFile);
        break;
      default:
        LOG.warn("file type is {}, not add in node", fileType);
        // ignore the object
    }
  }

  public List<DataFile> dataFiles() {
    dataFiles.clear();
    Iterables.addAll(dataFiles, baseFiles);
    Iterables.addAll(dataFiles, insertFiles);
    return dataFiles;
  }

  public List<ContentFile<?>> files() {
    allFiles.clear();
    Iterables.addAll(allFiles, baseFiles);
    Iterables.addAll(allFiles, insertFiles);
    Iterables.addAll(allFiles, deleteFiles);
    Iterables.addAll(allFiles, posDeleteFiles);
    for (IcebergContentFile icebergDataFile : icebergDataFiles) {
      allFiles.add(icebergDataFile.getContentFile());
    }
    for (IcebergContentFile icebergDataFile : icebergSmallDataFiles) {
      allFiles.add(icebergDataFile.getContentFile());
    }
    for (IcebergContentFile icebergDataFile : icebergEqDeleteFiles) {
      allFiles.add(icebergDataFile.getContentFile());
    }
    for (IcebergContentFile icebergDataFile : icebergPosDeleteFiles) {
      allFiles.add(icebergDataFile.getContentFile());
    }
    return allFiles;
  }

  public List<DataFile> baseFiles() {
    return baseFiles;
  }

  public List<DataFile> insertFiles() {
    return insertFiles;
  }

  public List<DataFile> deleteFiles() {
    return deleteFiles;
  }

  public List<DeleteFile> posDeleteFiles() {
    return posDeleteFiles;
  }

  public List<IcebergContentFile> icebergDataFiles() {
    return icebergDataFiles;
  }

  public List<IcebergContentFile> icebergSmallDataFiles() {
    return icebergSmallDataFiles;
  }

  public List<IcebergContentFile> allIcebergDataFiles() {
    List<IcebergContentFile> allIcebergDataFiles = Lists.newArrayList();
    allIcebergDataFiles.addAll(icebergDataFiles);
    allIcebergDataFiles.addAll(icebergSmallDataFiles);
    return allIcebergDataFiles;
  }

  public List<IcebergContentFile> allIcebergDeleteFiles() {
    List<IcebergContentFile> allIcebergDeleteFiles = Lists.newArrayList();
    allIcebergDeleteFiles.addAll(icebergEqDeleteFiles);
    allIcebergDeleteFiles.addAll(icebergPosDeleteFiles);
    return allIcebergDeleteFiles;
  }

  public Set<DataTreeNode> getSourceNodes() {
    return sourceNodes;
  }

  public void setSourceNodes(Set<DataTreeNode> sourceNodes) {
    this.sourceNodes = sourceNodes;
  }

  public OptimizeTaskId getTaskId() {
    return taskId;
  }

  public void setTaskId(OptimizeTaskId taskId) {
    this.taskId = taskId;
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public void setTableIdentifier(TableIdentifier tableIdentifier) {
    this.tableIdentifier = tableIdentifier;
  }

  public int getAttemptId() {
    return attemptId;
  }

  public void setAttemptId(int attemptId) {
    this.attemptId = attemptId;
  }

  public String getCustomHiveSubdirectory() {
    return customHiveSubdirectory;
  }

  public void setCustomHiveSubdirectory(String customHiveSubdirectory) {
    this.customHiveSubdirectory = customHiveSubdirectory;
  }

  public Long getMaxExecuteTime() {
    return maxExecuteTime;
  }

  public void setMaxExecuteTime(Long maxExecuteTime) {
    this.maxExecuteTime = maxExecuteTime;
  }

  public OptimizeType getOptimizeType() {
    return taskId.getType();
  }


  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("taskId", taskId)
        .add("attemptId", attemptId)
        .add("tableIdentifier", tableIdentifier)
        .add("baseFiles", baseFiles.size())
        .add("insertFiles", insertFiles.size())
        .add("deleteFiles", deleteFiles.size())
        .add("posDeleteFiles", posDeleteFiles.size())
        .add("icebergDataFiles", icebergDataFiles.size())
        .add("icebergSmallDataFiles", icebergSmallDataFiles.size())
        .add("icebergEqDeleteFiles", icebergEqDeleteFiles.size())
        .add("icebergPosDeleteFiles", icebergPosDeleteFiles.size())
        .add("customHiveSubdirectory", customHiveSubdirectory)
        .add("maxExecuteTime", maxExecuteTime)
        .toString();
  }
}