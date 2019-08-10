package com.xiaomi.infra;

import java.util.ArrayList;
import java.util.List;

public class Utils {

  public static List<String> tablePaths = new ArrayList<>();

  public static List<String> getTablePaths(String backUpRoot, String backUpCluster,
      String backUpPolicyName, String backUpPolicyId,
      String TableName, String TableId, int partitionCounter) {
    partitionCounter = partitionCounter - 1;
    while (partitionCounter >= 0) {
      tablePaths.add(
          backUpRoot + "/" + backUpCluster + "/" + backUpPolicyName + "/" + backUpPolicyId + "/" +
              TableName + "_" + TableId + "/" + partitionCounter);
      partitionCounter--;
    }
    return tablePaths;
  }

  public static List<String> getTablePaths(String tablePath, int partitionCounter) {
    partitionCounter = partitionCounter - 1;
    while (partitionCounter >= 0) {
      tablePaths.add(tablePath + "/" + partitionCounter+"/"+"chkpt_10.239.35.206_34803");
      partitionCounter--;
    }
    return tablePaths;
  }
}
