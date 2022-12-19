package com.netease.arctic.ams.server.rocksdb;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * This interface provides the map interface for storing records in disk after they
 * spill over from memory. Used by {@link ExternalSpillableMap}.
 *
 * @param <T> The generic type of the keys
 * @param <R> The generic type of the values
 */
public abstract class DiskMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Iterable<R> {

  private static final Logger LOG = LogManager.getLogger(DiskMap.class);
  // private final File diskMapPathFile;
  // private transient Thread shutdownThread = null;

  // Base path for the write file
  protected final String diskMapPath;

  public DiskMap(String basePath, String prefix) {
    this.diskMapPath = String.format("%s/%s", basePath, prefix);
    // diskMapPathFile = new File(diskMapPath);
    // FileUtils.deleteDirectory(diskMapPathFile);
    // Make sure the folder is deleted when JVM exits
    // diskMapPathFile.deleteOnExit();
    // addShutDownHook();
  }

  /**
   * Register shutdown hook to force flush contents of the data written to FileOutputStream from OS page cache
   * (typically 4 KB) to disk.
   */
  // private void addShutDownHook() {
  //   shutdownThread = new Thread(this::cleanup);
  //   Runtime.getRuntime().addShutdownHook(shutdownThread);
  // }

  /**
   * @returns a stream of the values stored in the disk.
   */
  abstract Stream<R> valueStream();

  /**
   * Number of bytes spilled to disk.
   */
  abstract long sizeOfFileOnDiskInBytes();

  /**
   * Close and cleanup the Map.
   */
  public void close() {
    cleanup(false);
  }

  /**
   * Cleanup all resources, files and folders
   * triggered by shutdownhook.
   */
  private void cleanup() {
    cleanup(true);
  }

  /**
   * Cleanup all resources, files and folders.
   */
  private void cleanup(boolean isTriggeredFromShutdownHook) {
    // try {
    //   FileUtils.deleteDirectory(diskMapPathFile);
    // } catch (IOException exception) {
    //   LOG.warn("Error while deleting the disk map directory=" + diskMapPath, exception);
    // }
    // if (!isTriggeredFromShutdownHook && shutdownThread != null) {
    //   Runtime.getRuntime().removeShutdownHook(shutdownThread);
    // }
  }
}