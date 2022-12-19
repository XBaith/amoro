package com.netease.arctic.ams.server.rocksdb;

public interface SizeEstimator<T> {

  /**
   * This method is used to estimate the size of a payload in memory. The default implementation returns the total
   * allocated size, in bytes, of the object and all other objects reachable from it
   */
  long sizeEstimate(T t);
}
