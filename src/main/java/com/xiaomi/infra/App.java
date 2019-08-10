package com.xiaomi.infra;

import org.rocksdb.Env;
import org.rocksdb.HdfsEnv;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Hello world!
 */
public class App {

  static {
    RocksDB.loadLibrary();
  }

  public static void main(String[] args) throws RocksDBException {
    try (Env env = new HdfsEnv("hdfs://localhost:9000");
        final Options options = new Options()
            .setDisableAutoCompactions(true)
            .setCreateIfMissing(true)
            .setEnv(env)
            .setLevel0FileNumCompactionTrigger(-1)) {
      Rocksdbjava rocksdbjava = new Rocksdbjava(options,
          "/backup/c4tst-perfomance/gensst/1563447771967/coldbackuptest_61",
          "chkpt_10.239.35.206_34803", 4);
      Scanner scanner = rocksdbjava.getRocksScanner();
      for (scanner.seekToFirst(); scanner.hasNext(); scanner.next()) {
        System.out.println(new String(scanner.key()) + ":" + new String(scanner.value()));
      }
      rocksdbjava.close();
    }
  }
}