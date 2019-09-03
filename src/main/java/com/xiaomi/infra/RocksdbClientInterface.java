package com.xiaomi.infra;

import com.xiaomi.infra.client.RocksdbScanner;

public interface RocksdbClientInterface {

  /**
   * get rocksdb scanner from hdfs
   * @return RocksdbScanner
   */
  public RocksdbScanner getRocksScanner();

}
