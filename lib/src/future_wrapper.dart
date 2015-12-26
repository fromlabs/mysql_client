// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.future_wrapper;

import 'dart:async';

class FutureWrapper<T> {
  var _wrapped;

  FutureWrapper([this._wrapped]);

  FutureWrapper.reusable();

  FutureWrapper<T> reuse([wrapped]) {
    _wrapped = wrapped;
    return this;
  }

  void free() {
    _wrapped = null;
  }

  bool get isFuture => _wrapped is Future;

  T get asValue => _wrapped;

  Future<T> get asFuture =>
      _wrapped is Future ? _wrapped : new Future.value(_wrapped);

  then(onValue(T value)) =>
      _wrapped is Future ? _wrapped.then(onValue) : onValue(_wrapped);

  Future thenFuture(onValue(T value)) {
    var value = then(onValue);
    return value is Future ? value : new Future.value(value);
  }
}
