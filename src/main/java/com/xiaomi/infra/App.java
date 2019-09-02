package com.xiaomi.infra;

import org.rocksdb.Env;
import org.rocksdb.HdfsEnv;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Hello world!
 */
public class App {

  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) {
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
          //System.out.println(hashKey + ":" + sortKey + "=>" + value);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
