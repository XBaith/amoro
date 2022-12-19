package com.netease.arctic.ams.server.rocksdb;

import java.io.Serializable;

public class SerializeWrapper<T> implements Serializable {
  private T value;

  public SerializeWrapper() {
    this(null);
  }

  public SerializeWrapper(T value) {
    this.value = value;
  }

  public T getValue(){
    return value;
  }
}
