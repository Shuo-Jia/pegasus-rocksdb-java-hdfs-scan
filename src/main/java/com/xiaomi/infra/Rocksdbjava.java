package com.xiaomi.infra;

import java.util.HashMap;
import java.util.Map;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class Rocksdbjava implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private Map<RocksDB, RocksIterator> rocksDBMap = new HashMap<>();
  private List<RocksIterator> rocksIteratorList = new ArrayList<>();

  public Rocksdbjava() {
  }

  public Rocksdbjava(Options options, String tableNamePath, String checkpointName,
      int partitionCounter)
      throws RocksDBException {
    List<String> checkPointPath = getCheckPointPath(tableNamePath, checkpointName,
        partitionCounter);
    for (String path : checkPointPath) {
      rocksIteratorList.add(getRocksIterator(options, path));
    }
  }

  public Scanner getRocksScanner() {
    return new Scanner(rocksIteratorList);
  }

  //todo
  public RocksIterator getRocksIterator(Options options, String checkPointPath)
      throws RocksDBException {
    final RocksDB db = RocksDB.open(options, checkPointPath);
    final RocksIterator rocksIterator = db.newIterator(new ReadOptions());
    rocksDBMap.put(db, rocksIterator);
    return rocksIterator;
  }

  private static List<String> getCheckPointPath(String tablePath, String checkpointName,
      int partitionCounter) {
    List<String> tablePaths = new ArrayList<>();
    partitionCounter = partitionCounter - 1;
    while (partitionCounter >= 0) {
      tablePaths.add(tablePath + "/" + partitionCounter + "/" + checkpointName);
      partitionCounter--;
    }
    return tablePaths;
  }

  @Override
  public void close() throws Exception {
    for (Map.Entry<RocksDB, RocksIterator> entry : rocksDBMap.entrySet()) {
      entry.getValue().close();
      entry.getKey().close();
    }
  }

  //todo

  /**
   * public RocksDB getRocksdb(Options options, String path) { try (final RocksDB db =
   * RocksDB.open(options, path)) { return db; } catch (final RocksDBException e) {
   * e.printStackTrace(); } return null; }
   **/


}
