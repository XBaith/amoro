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

package com.netease.arctic.ams.server.optimize;

import com.google.common.collect.ArrayListMultimap;
import com.netease.arctic.ams.api.ErrorMessage;
import com.netease.arctic.ams.api.InvalidObjectException;
import com.netease.arctic.ams.api.NoSuchObjectException;
import com.netease.arctic.ams.api.OptimizeStatus;
import com.netease.arctic.ams.api.OptimizeTaskId;
import com.netease.arctic.ams.api.OptimizeTaskStat;
import com.netease.arctic.ams.server.mapper.OptimizeHistoryMapper;
import com.netease.arctic.ams.server.mapper.OptimizeTaskRuntimesMapper;
import com.netease.arctic.ams.server.mapper.OptimizeTasksMapper;
import com.netease.arctic.ams.server.mapper.TableOptimizeRuntimeMapper;
import com.netease.arctic.ams.server.model.BaseOptimizeTask;
import com.netease.arctic.ams.server.model.BaseOptimizeTaskRuntime;
import com.netease.arctic.ams.server.model.OptimizeHistory;
import com.netease.arctic.ams.server.model.TableMetadata;
import com.netease.arctic.ams.server.model.TableOptimizeRuntime;
import com.netease.arctic.ams.server.service.IJDBCService;
import com.netease.arctic.ams.server.service.IMetaService;
import com.netease.arctic.ams.server.service.ServiceContainer;
import com.netease.arctic.ams.server.service.impl.OptimizeQueueService;
import com.netease.arctic.ams.server.utils.OptimizeStatusUtil;
import com.netease.arctic.ams.server.utils.ScheduledTasks;
import com.netease.arctic.ams.server.utils.ThreadPool;
import com.netease.arctic.catalog.ArcticCatalog;
import com.netease.arctic.table.ArcticTable;
import com.netease.arctic.table.TableIdentifier;
import com.netease.arctic.table.TableProperties;
import com.netease.arctic.utils.CatalogUtil;
import com.netease.arctic.utils.CompatiblePropertyUtil;
import java.util.concurrent.Executors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class OptimizeService extends IJDBCService implements IOptimizeService {
  private static final Logger LOG = LoggerFactory.getLogger(OptimizeService.class);

  private ScheduledTasks<TableIdentifier, OptimizeCheckTask> checkTasks;

  private final BlockingQueue<TableOptimizeItem> toCommitTables = new ArrayBlockingQueue<>(1000);

  private final ConcurrentHashMap<TableIdentifier, TableOptimizeItem> cachedTables = new ConcurrentHashMap<>();

  private final OptimizeQueueService optimizeQueueService;
  private final IMetaService metaService;
  private volatile boolean inited = false;

  public OptimizeService() {
    super();
    optimizeQueueService = ServiceContainer.getOptimizeQueueService();
    metaService = ServiceContainer.getMetaService();
    init();
  }

  private void init() {
    try {
      new Thread(() -> {
        try {
          LOG.info("OptimizeService init...");
          loadTables();
          initOptimizeTasksIntoOptimizeQueue();
          LOG.info("OptimizeService init completed");
        } catch (Exception e) {
          LOG.error("OptimizeService init failed", e);
        } finally {
          inited = true;
        }
      }).start();
      LOG.info("OptimizeService init async");
    } catch (Exception e) {
      LOG.error("OptimizeService init failed", e);
    }
  }

  @Override
  public synchronized void checkOptimizeCheckTasks(long checkInterval) {
    try {
      LOG.info("Schedule Optimize Checker");
      if (!inited) {
        LOG.info("OptimizeService init not completed, not check optimize task");
        return;
      }
      if (checkTasks == null) {
        checkTasks = new ScheduledTasks<>(ThreadPool.Type.OPTIMIZE_CHECK);
      }
      List<TableIdentifier> validTables = refreshAndListTables();
      checkTasks.checkRunningTask(
          new HashSet<>(validTables),
          identifier -> checkInterval,
          OptimizeCheckTask::new,
          false);
      LOG.info("Schedule Optimize Checker finished with {} valid tables", validTables.size());
    } catch (Throwable t) {
      LOG.error("unexpected error when checkOptimizeCheckTasks", t);
    }
  }

  @Override
  public List<TableIdentifier> listCachedTables() {
    return new ArrayList<>(cachedTables.keySet());
  }

  @Override
  public List<TableIdentifier> refreshAndListTables() {
    LOG.info("refresh tables");
    if (!inited) {
      LOG.info("OptimizeService init not completed, not refresh");
      return new ArrayList<>(cachedTables.keySet());
    }
    Set<TableIdentifier> tableIdentifiers =
        new HashSet<>(com.netease.arctic.ams.server.utils.CatalogUtil.loadTablesFromCatalog());
    List<TableIdentifier> toAddTables = tableIdentifiers.stream()
        .filter(t -> !cachedTables.containsKey(t))
        .collect(Collectors.toList());
    List<TableIdentifier> toRemoveTables = cachedTables.keySet().stream()
        .filter(t -> !tableIdentifiers.contains(t))
        .collect(Collectors.toList());

    addNewTables(toAddTables);
    clearRemovedTables(toRemoveTables);

    return new ArrayList<>(cachedTables.keySet());
  }

  private void clearTableCache(TableIdentifier tableIdentifier) {
    TableOptimizeItem tableItem = cachedTables.remove(tableIdentifier);
    optimizeQueueService.release(tableIdentifier);
    try {
      deleteTableOptimizeRuntime(tableIdentifier);
    } catch (Throwable t) {
      LOG.debug("failed to delete  " + tableIdentifier + " runtime, ignore", t);
    }
    try {
      tableItem.clearOptimizeTasks();
    } catch (Throwable t) {
      LOG.debug("failed to delete " + tableIdentifier + " optimize task, ignore", t);
    }
    try {
      deleteOptimizeRecord(tableIdentifier);
      deleteOptimizeTaskHistory(tableIdentifier);
    } catch (Throwable t) {
      LOG.debug("failed to delete " + tableIdentifier + " optimize(task) history, ignore", t);
    }
  }

  private void addTableIntoCache(TableOptimizeItem arcticTableItem, Map<String, String> properties,
                                 boolean persistRuntime) {
    cachedTables.put(arcticTableItem.getTableIdentifier(), arcticTableItem);
    try {
      String groupName = CompatiblePropertyUtil.propertyAsString(properties,
          TableProperties.SELF_OPTIMIZING_GROUP, TableProperties.SELF_OPTIMIZING_GROUP_DEFAULT);
      optimizeQueueService.bind(arcticTableItem.getTableIdentifier(), groupName);
    } catch (InvalidObjectException e) {
      LOG.debug("failed to bind " + arcticTableItem.getTableIdentifier() + " and queue ", e);
    }
    if (persistRuntime) {
      try {
        insertTableOptimizeRuntime(arcticTableItem.getTableOptimizeRuntime());
      } catch (Throwable t) {
        LOG.debug("failed to insert " + arcticTableItem.getTableIdentifier() + " runtime, ignore", t);
      }
    }
  }

  @Override
  public TableOptimizeItem getTableOptimizeItem(TableIdentifier tableIdentifier) throws NoSuchObjectException {
    TableOptimizeItem tableOptimizeItem = cachedTables.get(tableIdentifier);
    if (tableOptimizeItem == null) {
      refreshAndListTables();
      TableOptimizeItem reloadTableOptimizeItem = cachedTables.get(tableIdentifier);
      if (reloadTableOptimizeItem == null) {
        throw new NoSuchObjectException("can't find table " + tableIdentifier);
      }
      return reloadTableOptimizeItem;
    }
    return tableOptimizeItem;
  }

  @Override
  public void handleOptimizeResult(OptimizeTaskStat optimizeTaskStat) throws NoSuchObjectException {
    getTableOptimizeItem(new TableIdentifier(optimizeTaskStat.getTableIdentifier()))
        .updateOptimizeTaskStat(optimizeTaskStat);
  }

  private void loadTables() {
    LOG.info("init load tables");
    // load table when server start, only load table metadata
    Map<TableIdentifier, List<OptimizeTaskItem>> optimizeTasks = loadOptimizeTasks();
    Map<TableIdentifier, TableOptimizeRuntime> tableOptimizeRuntimes = loadTableOptimizeRuntimes();

    // load tables from catalog
    Set<TableIdentifier> tableIdentifiers = com.netease.arctic.ams.server.utils.CatalogUtil.loadTablesFromCatalog();

    Tasks.foreach(tableIdentifiers)
        .suppressFailureWhenFinished()
        .noRetry()
        .executeWith(Executors.newFixedThreadPool(5))
        .run(tableIdentifier -> {
          List<OptimizeTaskItem> tableOptimizeTasks = optimizeTasks.remove(tableIdentifier);

          ArcticCatalog arcticCatalog =
              com.netease.arctic.ams.server.utils.CatalogUtil.getArcticCatalog(tableIdentifier.getCatalog());
          ArcticTable arcticTable = arcticCatalog.loadTable(tableIdentifier);

          TableMetadata tableMetadata = buildTableMetadata(arcticCatalog, arcticTable);

          if (CompatiblePropertyUtil.propertyAsBoolean(tableMetadata.getProperties(), TableProperties.ENABLE_SELF_OPTIMIZING,
              TableProperties.ENABLE_SELF_OPTIMIZING_DEFAULT)) {
            TableOptimizeItem arcticTableItem = new TableOptimizeItem(null, tableMetadata);
            TableOptimizeRuntime oldTableOptimizeRuntime = tableOptimizeRuntimes.remove(tableIdentifier);
            arcticTableItem.initTableOptimizeRuntime(oldTableOptimizeRuntime)
                .initOptimizeTasks(tableOptimizeTasks);
            addTableIntoCache(arcticTableItem, tableMetadata.getProperties(), oldTableOptimizeRuntime == null);
          }
        });

    if (!optimizeTasks.isEmpty()) {
      LOG.warn("clear optimize tasks {}", optimizeTasks.keySet());
      for (Map.Entry<TableIdentifier, List<OptimizeTaskItem>> entry : optimizeTasks.entrySet()) {
        for (OptimizeTaskItem task : entry.getValue()) {
          task.clearOptimizeTask();
        }
      }
    }

    if (!tableOptimizeRuntimes.isEmpty()) {
      LOG.warn("clear table runtime {}", tableOptimizeRuntimes.keySet());
      for (TableIdentifier tableIdentifier : tableOptimizeRuntimes.keySet()) {
        deleteTableOptimizeRuntime(tableIdentifier);
      }
    }
  }

  private TableMetadata buildTableMetadata(ArcticCatalog arcticCatalog, ArcticTable arcticTable) {
    TableIdentifier tableIdentifier = arcticTable.id();
    TableMetadata tableMetadata = new TableMetadata();
    if (CatalogUtil.isIcebergCatalog(arcticCatalog)) {
      tableMetadata.setTableIdentifier(tableIdentifier);
      tableMetadata.setProperties(arcticTable.properties());
    } else {
      tableMetadata = metaService.loadTableMetadata(tableIdentifier);
    }

    return tableMetadata;
  }

  private void initOptimizeTasksIntoOptimizeQueue() {
    ArrayListMultimap<Integer, OptimizeTaskItem> multiMap = ArrayListMultimap.create();
    cachedTables.values().stream().flatMap(t -> t.getOptimizeTasks().stream())
        .filter(t -> OptimizeStatusUtil.in(t.getOptimizeStatus(), OptimizeStatus.Pending))
        .forEach(t -> multiMap.put(t.getOptimizeTask().getQueueId(), t));
    for (Integer queueId : multiMap.keySet()) {
      List<OptimizeTaskItem> optimizeTaskItems = multiMap.get(queueId);
      optimizeTaskItems
          .sort(Comparator.comparingLong(o -> o.getOptimizeRuntime().getPendingTime()));
      for (OptimizeTaskItem task : optimizeTaskItems) {
        try {
          optimizeQueueService.submitTask(task);
        } catch (NoSuchObjectException | InvalidObjectException e) {
          LOG.error("failed to load task {} into optimizeQueue {}", task.getOptimizeTask(), queueId);
          task.onFailed(new ErrorMessage(System.currentTimeMillis(), e.getMessage()), 0L);
        }
      }
    }
  }

  @Override
  public void addNewTables(List<TableIdentifier> toAddTables) {
    if (CollectionUtils.isEmpty(toAddTables)) {
      return;
    }
    if (!inited) {
      LOG.info("OptimizeService init not completed, can't add new tables");
      return;
    }

    toAddTables.stream().map(toAddTable -> {
      ArcticCatalog arcticCatalog =
          com.netease.arctic.ams.server.utils.CatalogUtil.getArcticCatalog(toAddTable.getCatalog());
        ArcticTable arcticTable;
        try {
          arcticTable = arcticCatalog.loadTable(toAddTable);
          return Pair.of(arcticTable, buildTableMetadata(arcticCatalog, arcticTable));
        } catch (Exception e) {
          LOG.error("Fail to load table {}", toAddTable);
        }

        return null;
    }).filter(tuple -> tuple != null &&
        CompatiblePropertyUtil.propertyAsBoolean(
            tuple.getValue().getProperties(),
            TableProperties.ENABLE_SELF_OPTIMIZING,
            TableProperties.ENABLE_SELF_OPTIMIZING_DEFAULT)
    ).forEach(tuple -> {
        ArcticTable arcticTable = tuple.getKey();
        TableMetadata tableMetadata = tuple.getValue();
        TableOptimizeItem newTableItem = new TableOptimizeItem(arcticTable, tableMetadata);
        long createTime =
            PropertyUtil.propertyAsLong(tableMetadata.getProperties(), TableProperties.TABLE_CREATE_TIME,
                TableProperties.TABLE_CREATE_TIME_DEFAULT);
        newTableItem.getTableOptimizeRuntime().setOptimizeStatusStartTime(createTime);
        addTableIntoCache(newTableItem, arcticTable.properties(), true);
      LOG.info("Add a new table: {}", arcticTable.id());
    });
  }

  @Override
  public void clearRemovedTables(List<TableIdentifier> toRemoveTables) {
    if (CollectionUtils.isEmpty(toRemoveTables)) {
      return;
    }
    if (!inited) {
      LOG.info("OptimizeService init not completed, can't add new tables");
      return;
    }
    toRemoveTables.forEach(this::clearTableCache);
    LOG.info("clear tables[{}] {}", toRemoveTables.size(), toRemoveTables);
  }

  private Map<TableIdentifier, List<OptimizeTaskItem>> loadOptimizeTasks() {
    Map<TableIdentifier, List<OptimizeTaskItem>> results = new HashMap<>();

    List<BaseOptimizeTask> optimizeTasks = selectAllOptimizeTasks();

    for (BaseOptimizeTask optimizeTask : optimizeTasks) {
      initOptimizeTask(optimizeTask);
    }
    Map<OptimizeTaskId, BaseOptimizeTaskRuntime> optimizeTaskRuntimes =
        selectAllOptimizeTaskRuntimes().stream()
            .collect(Collectors.toMap(BaseOptimizeTaskRuntime::getOptimizeTaskId, r -> r));
    AtomicBoolean lostTaskRuntime = new AtomicBoolean(false);
    List<OptimizeTaskItem> optimizeTaskItems = optimizeTasks.stream()
        .map(t -> {
          BaseOptimizeTaskRuntime optimizeTaskRuntime = optimizeTaskRuntimes.get(t.getTaskId());
          if (optimizeTaskRuntime == null) {
            lostTaskRuntime.set(true);
            LOG.error("can't find optimize task runtime in sysdb, tableIdentifier = {}, taskId = {}",
                t.getTableIdentifier(), t.getTaskId());
          }
          return new OptimizeTaskItem(t,
              optimizeTaskRuntimes.getOrDefault(t.getTaskId(), new BaseOptimizeTaskRuntime(t.getTaskId())));
        })
        .collect(Collectors.toList());

    if (lostTaskRuntime.get()) {
      throw new IllegalStateException("sysdb error, lost some task runtimes, fix sysdb first");
    }

    for (OptimizeTaskItem optimizeTaskItem : optimizeTaskItems) {
      TableIdentifier tableIdentifier = optimizeTaskItem.getTableIdentifier();
      List<OptimizeTaskItem> optimizeTaskItemList = results.computeIfAbsent(tableIdentifier, e -> new ArrayList<>());
      optimizeTaskItemList.add(optimizeTaskItem);
    }
    return results;
  }

  private void initOptimizeTask(BaseOptimizeTask optimizeTask) {
    if (optimizeTask.getInsertFiles() == null) {
      optimizeTask.setInsertFiles(Collections.emptyList());
    }
    if (optimizeTask.getDeleteFiles() == null) {
      optimizeTask.setDeleteFiles(Collections.emptyList());
    }
    if (optimizeTask.getBaseFiles() == null) {
      optimizeTask.setBaseFiles(Collections.emptyList());
    }
    if (optimizeTask.getPosDeleteFiles() == null) {
      optimizeTask.setPosDeleteFiles(Collections.emptyList());
    }
  }

  private Map<TableIdentifier, TableOptimizeRuntime> loadTableOptimizeRuntimes() {
    Map<TableIdentifier, TableOptimizeRuntime> collector = new HashMap<>();
    List<TableOptimizeRuntime> tableOptimizeRuntimes = selectTableOptimizeRuntimes();
    for (TableOptimizeRuntime runtime : tableOptimizeRuntimes) {
      collector.put(runtime.getTableIdentifier(), runtime);
    }
    return collector;
  }

  private void deleteOptimizeRecord(TableIdentifier tableIdentifier) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      OptimizeHistoryMapper optimizeHistoryMapper =
          getMapper(sqlSession, OptimizeHistoryMapper.class);
      optimizeHistoryMapper.deleteOptimizeRecord(tableIdentifier);
    }
  }

  private void deleteOptimizeTaskHistory(TableIdentifier tableIdentifier) {
    ServiceContainer.getTableTaskHistoryService().deleteTaskHistory(tableIdentifier);
  }

  @Override
  public List<OptimizeHistory> getOptimizeHistory(TableIdentifier identifier) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      OptimizeHistoryMapper optimizeHistoryMapper =
          getMapper(sqlSession, OptimizeHistoryMapper.class);

      return optimizeHistoryMapper.selectOptimizeHistory(identifier);
    }
  }

  @Override
  public long maxOptimizeHistoryId() {
    try (SqlSession sqlSession = getSqlSession(true)) {
      OptimizeHistoryMapper optimizeHistoryMapper =
          getMapper(sqlSession, OptimizeHistoryMapper.class);
      Long maxId = optimizeHistoryMapper.maxOptimizeHistoryId();
      return maxId == null ? 0 : maxId;
    }
  }

  @Override
  public boolean triggerOptimizeCommit(TableOptimizeItem tableOptimizeItem) {
    return toCommitTables.offer(tableOptimizeItem);
  }

  @Override
  public TableOptimizeItem takeTableToCommit() throws InterruptedException {
    return toCommitTables.take();
  }

  @Override
  public void expireOptimizeHistory(TableIdentifier tableIdentifier, long expireTime) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      OptimizeHistoryMapper optimizeHistoryMapper =
          getMapper(sqlSession, OptimizeHistoryMapper.class);
      optimizeHistoryMapper.expireOptimizeHistory(tableIdentifier, expireTime);
    }
  }

  private List<BaseOptimizeTaskRuntime> selectAllOptimizeTaskRuntimes() {
    try (SqlSession sqlSession = getSqlSession(true)) {
      OptimizeTaskRuntimesMapper optimizeTaskRuntimesMapper =
          getMapper(sqlSession, OptimizeTaskRuntimesMapper.class);
      return optimizeTaskRuntimesMapper.selectAllOptimizeTaskRuntimes();
    }
  }

  private List<BaseOptimizeTask> selectAllOptimizeTasks() {
    try (SqlSession sqlSession = getSqlSession(true)) {
      OptimizeTasksMapper optimizeTasksMapper =
          getMapper(sqlSession, OptimizeTasksMapper.class);
      return optimizeTasksMapper.selectAllOptimizeTasks();
    }
  }

  // table runtime
  private void insertTableOptimizeRuntime(TableOptimizeRuntime tableOptimizeRuntime) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      TableOptimizeRuntimeMapper tableOptimizeRuntimeMapper =
          getMapper(sqlSession, TableOptimizeRuntimeMapper.class);
      tableOptimizeRuntimeMapper.insertTableOptimizeRuntime(tableOptimizeRuntime);
    }
  }

  private void deleteTableOptimizeRuntime(TableIdentifier tableIdentifier) {
    try (SqlSession sqlSession = getSqlSession(true)) {
      TableOptimizeRuntimeMapper tableOptimizeRuntimeMapper =
          getMapper(sqlSession, TableOptimizeRuntimeMapper.class);
      tableOptimizeRuntimeMapper.deleteTableOptimizeRuntime(tableIdentifier);
    }
  }

  private List<TableOptimizeRuntime> selectTableOptimizeRuntimes() {
    try (SqlSession sqlSession = getSqlSession(true)) {
      TableOptimizeRuntimeMapper tableOptimizeRuntimeMapper =
          getMapper(sqlSession, TableOptimizeRuntimeMapper.class);
      return tableOptimizeRuntimeMapper.selectTableOptimizeRuntimes();
    }
  }
}