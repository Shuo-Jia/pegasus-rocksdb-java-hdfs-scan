package com.xiaomi.infra;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.rocksdb.Env;
import org.rocksdb.HdfsEnv;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

/**
 * Unit test for simple App.
 */
public class AppTest {

  static {
    RocksDB.loadLibrary();
  }

  /**
   * Rigorous Test :-)
   */
  @Test
  public void scanAll() {
    try (Env env = new HdfsEnv("hdfs://localhost:9000");
        final Options options = new Options()
            .setDisableAutoCompactions(true)
            .setCreateIfMissing(true)
            .setEnv(env)
            .setLevel0FileNumCompactionTrigger(-1)) {
      try (Rocksdbjava rocksdbjava = new Rocksdbjava(options,
          "/backup/scanner/1567419358655/scanner_2/",
          "chkpt_10.239.35.206_34801",
          4)) {
        Scanner scanner = rocksdbjava.getRocksScanner();
        for (scanner.seekToFirst(); scanner.hasNext(); scanner.next()) {
          PegasusKey pegasusKey = scanner.key();
          String hashKey = new String(pegasusKey.hashKey);
          String sortKey = new String(pegasusKey.sortKey);
          String value = new String(scanner.value());
          System.out.println(hashKey + ":" + sortKey + "=>" + value);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void scanOne() {
    int partitionCount = 4;
    try (Env env = new HdfsEnv("hdfs://localhost:9000");
        final Options options = new Options()
            .setDisableAutoCompactions(true)
            .setCreateIfMissing(true)
            .setEnv(env)
            .setLevel0FileNumCompactionTrigger(-1)) {
      try (Rocksdbjava rocksdbjava = new Rocksdbjava()) {
        partitionCount--;
        while (partitionCount >= 0) {
          System.out.println("***********" + partitionCount + "***************");
          RocksIterator rocksIterator = rocksdbjava.getRocksIterator(
              options,
              "/backup/scanner/1567419358655/scanner_2/" + partitionCount
                  + "/chkpt_10.239.35.206_34801");
          for (rocksIterator.seekToFirst(); rocksIterator.isValid(); rocksIterator.next()) {
            byte[] pegasusKey = rocksIterator.key();
            if (pegasusKey != null && pegasusKey.length >= 2) {
              Pair<byte[], byte[]> pair = Utils.restoreKey(pegasusKey);
              String hashKey = new String(pair.getLeft());
              String sortKey = new String(pair.getRight());
              String value = new String(Utils.restoreValue(rocksIterator.value()));
              //System.out.println(hashKey + ":" + sortKey + "=>" + value);
            }
          }
          partitionCount--;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
