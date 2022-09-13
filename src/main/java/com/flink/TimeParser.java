package com.flink;

import org.apache.flink.table.functions.ScalarFunction;

public class TimeParser extends ScalarFunction {
  public long eval(String a) {
      return a == null ? 0 : a.length();
  }

  public long eval(String b, String c) {
      return eval(b) + eval(c);
  }
}
