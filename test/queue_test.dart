import "dart:async";

import "package:stack_trace/stack_trace.dart";

import "package:mysql_client/mysql_client.dart";

Future main() async {
  await Chain.capture(() async {
    await capturedMain();
  }, onError: (e, s) {
    print(e);
    print(Trace.format(s));
  });
}

Future capturedMain() async {
  await test2();
}

Future test2() async {
  var pool;

  try {
    pool = new ConnectionPool(
        host: "localhost",
        port: 3306,
        userName: "root",
        password: "mysql",
        database: "test",
        maxConnections: 10,
        connectionTimeout: new Duration(seconds: 30));

    var connection = await pool.request();

    var f1 = connection
        .executeQuery("select count(*) from people")
        .then((result) async {
      await result.next();

      print(result.getNumValue(0));
    });

    var f2 = connection
        .executeQuery("select count(*) from people")
        .then((result) async {
      await result.next();

      print(result.getNumValue(0));
    });

    await Future.wait([f1, f2]);
  } finally {
    await pool.close();
  }
}
