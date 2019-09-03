package com.xiaomi.infra.client;

import com.xiaomi.infra.utils.Utils;
import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.RocksIterator;
import java.util.*;

public class RocksdbScanner {

  private Iterator<RocksIterator> scanners;
  private RocksIterator rocksIterator;


  public RocksdbScanner(List<RocksIterator> rocksIterators) {
    scanners = rocksIterators.iterator();
  }


  public RocksIterator next() {
    rocksIterator.next();
    if (rocksIterator.isValid()) {
      if (rocksIterator.key() == null || rocksIterator.key().length < 2) {
        rocksIterator.next();
      }
      return rocksIterator;
    } else {
      seekToFirst();
      return rocksIterator;
    }
  }


  public void seekToFirst() {
    if (scanners.hasNext()) {
      rocksIterator = scanners.next();
      rocksIterator.seekToFirst();
      //System.out.println("*****************" + rocksIterator + "**********************");
      if (rocksIterator.isValid()) {
        if (rocksIterator.key() == null || rocksIterator.key().length < 2) {
          rocksIterator.next();
        }
      } else {
        seekToFirst();
      }
    }
  }

  public boolean hasNext() {
    return scanners.hasNext();
  }

  public PegasusKey key() {
    Pair<byte[], byte[]> keyPair = Utils.restoreKey(rocksIterator.key());
    return new PegasusKey(keyPair.getLeft(), keyPair.getRight());
  }

  public byte[] value() {
    return Utils.restoreValue(rocksIterator.value());
  }

}