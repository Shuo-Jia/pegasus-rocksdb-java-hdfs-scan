package com.xiaomi.infra;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.*;

class Scanner {

  private Iterator<RocksIterator> scanners;
  private RocksIterator rocksIterator;


  public Scanner(List<RocksIterator> rocksIterators) {
    scanners = rocksIterators.iterator();
  }


  public RocksIterator next() {
    rocksIterator.next();
    if (rocksIterator.isValid()) {
      return rocksIterator;
    } else {
      seekToFirst();
      return rocksIterator;
    }
  }


  public void seekToFirst() {
    if (scanners.hasNext()) {
      rocksIterator = scanners.next();
      System.out.println("current partitionId iter:"+rocksIterator.toString());
      rocksIterator.seekToFirst();
    }
  }

  public boolean hasNext() {
    return scanners.hasNext();
  }

  public byte[] key() {
    return rocksIterator.key();
  }

  public byte[] value() {
    return rocksIterator.value();
  }

  public void free() {
  }
}