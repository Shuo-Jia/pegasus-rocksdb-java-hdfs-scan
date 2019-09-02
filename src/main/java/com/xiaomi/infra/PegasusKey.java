package com.xiaomi.infra;

public class PegasusKey {

  byte[] hashKey;
  byte[] sortKey;

  public PegasusKey(byte[] hashKey, byte[] sortKey) {
    this.hashKey = hashKey;
    this.sortKey = sortKey;
  }
}
