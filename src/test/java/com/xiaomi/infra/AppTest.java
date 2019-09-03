package com.xiaomi.infra;

import static org.junit.Assert.assertTrue;

import com.xiaomi.infra.client.PegasusKey;
import com.xiaomi.infra.client.RocksdbClient;
import com.xiaomi.infra.client.RocksdbScanner;
import com.xiaomi.infra.utils.Utils;
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
      try (RocksdbClient rocksdbClient = new RocksdbClient(options,
          "/backup/scanner/1567419358655/scanner_2/",
          "chkpt_10.239.35.206_34801",
          4)) {
        RocksdbScanner rocksdbScanner = rocksdbClient.getRocksScanner();
        for (rocksdbScanner.seekToFirst(); rocksdbScanner.hasNext(); rocksdbScanner.next()) {
          PegasusKey pegasusKey = rocksdbScanner.key();
          String hashKey = new String(pegasusKey.hashKey);
          String sortKey = new String(pegasusKey.sortKey);
          String value = new String(rocksdbScanner.value());
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
      try (RocksdbClient rocksdbClient = new RocksdbClient()) {
        partitionCount--;
        while (partitionCount >= 0) {
          System.out.println("***********" + partitionCount + "***************");
          RocksIterator rocksIterator = rocksdbClient.getRocksIterator(
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
