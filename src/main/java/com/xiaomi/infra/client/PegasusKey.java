package com.xiaomi.infra.client;

public class PegasusKey {

  public byte[] hashKey;
  public byte[] sortKey;

  public PegasusKey(byte[] hashKey, byte[] sortKey) {
    this.hashKey = hashKey;
    this.sortKey = sortKey;
  }
}
