package com.netease.arctic.ams.server.rocksdb;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A map's implementation based on RocksDB.
 */
public final class RocksDBBasedMap<K extends Serializable, R extends Serializable> implements Map<K, R>, Serializable {

  private static final String COL_FAMILY_NAME = "map_handle";

  private final String rocksDbStoragePath;
  private transient RocksDBDAO rocksDBDAO;
  private final String columnFamilyName;

  public RocksDBBasedMap(String rocksDbStoragePath) {
    this.rocksDbStoragePath = rocksDbStoragePath;
    this.columnFamilyName = COL_FAMILY_NAME;
  }

  @Override
  public int size() {
    return (int) getRocksDBDAO().prefixSearch(columnFamilyName, "").count();
  }

  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    // Wont be able to store nulls as values
    return getRocksDBDAO().get(columnFamilyName, key.toString()) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public R get(Object key) {
    return getRocksDBDAO().get(columnFamilyName, (Serializable) key);
  }

  @Override
  public R put(K key, R value) {
    getRocksDBDAO().put(columnFamilyName, key, value);
    return value;
  }

  @Override
  public R remove(Object key) {
    R val = getRocksDBDAO().get(columnFamilyName, key.toString());
    getRocksDBDAO().delete(columnFamilyName, key.toString());
    return val;
  }

  @Override
  public void putAll(Map<? extends K, ? extends R> m) {
    getRocksDBDAO().writeBatch(batch -> m.forEach((key, value) -> getRocksDBDAO().putInBatch(batch, columnFamilyName, key, value)));
  }

  private RocksDBDAO getRocksDBDAO() {
    if (null == rocksDBDAO) {
      rocksDBDAO = new RocksDBDAO(rocksDbStoragePath);
      rocksDBDAO.addColumnFamily(columnFamilyName);
    }
    return rocksDBDAO;
  }

  @Override
  public void clear() {
    if (null != rocksDBDAO) {
      rocksDBDAO.close();
    }
    rocksDBDAO = null;
  }

  @Override
  public Set<K> keySet() {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public Collection<R> values() {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public Set<Entry<K, R>> entrySet() {
    throw new UnsupportedOperationException("Not Supported");
  }

}
