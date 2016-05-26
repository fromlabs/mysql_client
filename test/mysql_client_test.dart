// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.test;

import "dart:async";

import 'package:mysql_client/mysql_client.dart';
import "package:stack_trace/stack_trace.dart";

// sudo ngrep -x -q -d lo0 '' 'port 3306'

Future main() async {
  Chain.capture(() async {
    try {
      await test5();
    } catch (e, s) {
      print(e);
      print(Trace.format(s));
    }
  });
}

Future test5() async {
  var connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    // column count
    var columnCount = queryResult.columnCount;
    print(columnCount);

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print(queryResult.getNumValue(0));
    }
  } finally {
    await connection.close();
  }
}

Future test20() async {
  var connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "mysql", "test");

    var queryResult;

    queryResult = await connection
        .executeQuery("UPDATE people SET age = 92 WHERE id = 9");

    print(queryResult.affectedRows);

    queryResult = await connection
        .executeQuery("UPDATE people SET age = 93 WHERE id = 9");

    print(queryResult.affectedRows);

    queryResult =
        await connection.executeQuery("SELECT * FROM people WHERE id = 9");

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print(
          "${queryResult.getNumValue(0)}: ${queryResult.getStringValue(1)}, ${queryResult.getStringValue(2)}");
    }
  } finally {
    await connection.close();
  }
}

Future test7() async {
  var connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "mysql", "test");

    var queryResult;

    queryResult = await connection
        .executeQuery("INSERT INTO people(name, age) values ('hans', 42)");

    print(queryResult.affectedRows);

    queryResult =
        await connection.executeQuery("SELECT * FROM people WHERE age = 42");

    print(queryResult.columns);

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print("${queryResult.getNumValue(0)}: ${queryResult.getStringValue(1)}");
    }
  } finally {
    await connection.close();
  }
}

Future test8() async {
  var connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "mysql", "test");

    var queryResult =
        await connection.executeQuery("SELECT * FROM people WHERE id = 10");

    print(queryResult.columns);
    print(queryResult.columnCount);

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print(
          "${queryResult.getNumValue(0)}: ${queryResult.getStringValue(1)}, ${queryResult.getNumValue(2)}");
    }
  } catch (e, s) {
    print("Error: $e");
    print(new Chain.forTrace(s).terse);
  } finally {
    await connection.close();
  }
}

Future test6() async {
  var connection;

  try {
    connection = await new ConnectionFactory()
        .connect("localhost", 3306, "root", "mysql", "test");

    var queryResult;

    queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 100");

    queryResult =
        await connection.executeQuery("SELECT * FROM people LIMIT 10");

    print(queryResult.columnCount);

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print(queryResult.getNumValue(0));
    }

    queryResult = await connection.executeQuery("SELECT * FROM people LIMIT 5");

    print(queryResult.columnCount);

    // rows
    while (true) {
      var next = await queryResult.next();
      if (!next) {
        break;
      }

      print(queryResult.getNumValue(0));
    }

    queryResult = await connection.executeQuery("SELECT * FROM people LIMIT 0");
  } catch (e, s) {
    print("Error: $e");
    print(new Chain.forTrace(s).terse);
  } finally {
    await connection.close();
  }
}
