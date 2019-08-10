package com.xiaomi.infra;

import java.util.HashMap;
import java.util.Map;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.List;

public class Rocksdbjava {

  static {
    RocksDB.loadLibrary();
  }

  private Map<RocksDB, RocksIterator> rocksDBMap = new HashMap<>();
  private List<RocksIterator> rocksIteratorList = new ArrayList<>();

  public Rocksdbjava() {
  }

  public Rocksdbjava(Options options, List<String> tablePaths) throws RocksDBException {
    for (String tablePath : tablePaths) {
      rocksIteratorList.add(getRocksIterator(options, tablePath));
    }
  }

  public Rocksdbjava(Options options, String tableNamePath, String checkpointName, int partitionCounter)
      throws RocksDBException {
    List<String> SSTPaths = getSSTPaths(tableNamePath, checkpointName, partitionCounter);
    for (String path : SSTPaths) {
      rocksIteratorList.add(getRocksIterator(options, path));
    }
  }

  public Scanner getRocksScanner() {
    return new Scanner(rocksIteratorList);
  }


  public void close() {
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

  //todo
  private RocksIterator getRocksIterator(Options options, String tablePath) throws RocksDBException {
    final RocksDB db = RocksDB.open(options, tablePath);
    final RocksIterator rocksIterator = db.newIterator(new ReadOptions());
    rocksDBMap.put(db, rocksIterator);
    return rocksIterator;
  }

  private static List<String> getSSTPaths(String tablePath, String checkpointName,
      int partitionCounter) {
    List<String> tablePaths = new ArrayList<>();
    partitionCounter = partitionCounter - 1;
    while (partitionCounter >= 0) {
      tablePaths.add(tablePath + "/" + partitionCounter + "/" + checkpointName);
      partitionCounter--;
    }
    return tablePaths;
  }
}
