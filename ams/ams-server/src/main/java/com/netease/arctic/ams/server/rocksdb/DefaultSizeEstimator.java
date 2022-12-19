package com.netease.arctic.ams.server.rocksdb;

import com.twitter.common.objectsize.ObjectSizeCalculator;
import java.io.Serializable;

/**
 * Default implementation of size-estimator that uses Twitter's ObjectSizeCalculator.
 *
 * @param <T>
 */
public class DefaultSizeEstimator<T> implements SizeEstimator<T>, Serializable {

  @Override
  public long sizeEstimate(T t) {
    return ObjectSizeCalculator.getObjectSize(t);
  }
}
