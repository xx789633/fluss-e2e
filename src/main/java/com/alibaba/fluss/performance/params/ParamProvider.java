package com.alibaba.fluss.performace.params;

public interface ParamProvider<T> {
  void init(String patter);

  T next();
}
