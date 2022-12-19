package com.netease.arctic.ams.server;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netease.arctic.ams.server.rocksdb.RocksDbDiskMap;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.iceberg.CatalogUtil.ICEBERG_CATALOG_TYPE;

public class RocksDBMapTest {

  private Map<String, String> rocksMap;

  private Map<String, ArrayList<FileScanTask>> rocksListMap;
  private Table table;

  @Before
  public void load() throws IOException {
    rocksMap = new RocksDbDiskMap<>("test");
    rocksListMap = new RocksDbDiskMap<>("file-scan");
    Map<String, String> prop = new ImmutableMap.Builder<String, String>()
        .put(ICEBERG_CATALOG_TYPE, "hive")
        .put(CatalogProperties.URI, "thrift://wapsj1dls001.webex.com:9083")
        .build();
    Configuration conf = new Configuration();
    conf.addResource(new Path("/Users/xuba/Downloads/config/dls/core-site.xml"));
    conf.addResource(new Path("/Users/xuba/Downloads/config/dls/hdfs-site.xml"));
    Catalog catalog = CatalogUtil.buildIcebergCatalog("iceberg", prop, conf);

    table = catalog.loadTable(TableIdentifier.of("pda", "assign_cohost_table"));
  }

  @Test
  public void testPutGet() {
    if (rocksMap.containsKey("key")) {
      Assert.assertEquals("value", rocksMap.get("key"));
    } else {
      throw new IllegalArgumentException("key not existing");
    }
    rocksMap.clear();
  }

  @Test
  public void testLists() {
    rocksListMap.put(table.name(), Lists.newArrayList(table.newScan().planFiles()));
  }

  @Test
  public void testGet() {
    rocksListMap.get(table.name())
        .forEach(t -> System.out.println(t.file().path()));
  }

  @Test
  public void testDelete() {
    rocksListMap.remove(table.name());
  }

}
