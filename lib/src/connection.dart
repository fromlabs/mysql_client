library mysql_client.connection;

import 'dart:async';

import "connection/connection_impl.dart";

enum SqlType {
  TINY,
  LONG,
  DOUBLE,
  NULL,
  TIMESTAMP,
  LONGLONG,
  DATETIME,
  VAR_STRING
}

class ConnectionError extends Error {
  final String message;

  ConnectionError(this.message);

  @override
  String toString() => "ConnectionError: $message";
}

class QueryError extends Error {
  final String message;

  QueryError(this.message);

  @override
  String toString() => "QueryError: $message";
}

class ColumnDefinition {
  final String name;
  final int type;

  ColumnDefinition(this.name, this.type);
}

abstract class ConnectionFactory {
  factory ConnectionFactory() {
    return new ConnectionFactoryImpl();
  }

  Future<Connection> connect(host, int port, String userName, String password,
      [String database]);
}

abstract class Closable {
  bool get isClosed;

  Future close();
}

abstract class Connection implements Closable {
  Future<QueryResult> executeQuery(String query);

  CommandRequest requestQueryExecution(String query);
}

abstract class CommandRequest implements Closable {
  Future<CommandResult> get response;
}

abstract class CommandResult implements Closable {}

abstract class DataIterator implements Closable {
  Future<bool> next();

  // TODO qui si potrebbe utilizzare il FutureWrapper
  rawNext();
}

abstract class RowIterator implements DataIterator {
  String getStringValue(int index);

  num getNumValue(int index);

  bool getBoolValue(int index);
}

abstract class QueryResult implements CommandResult, RowIterator {
  int get affectedRows;

  int get lastInsertId;

  int get columnCount;

  List<ColumnDefinition> get columns;

  // TODO aggiungere skip e limit
  // TODO aggiungere hint tipo sql per il recupero
  Future<List<List>> getNextRows();
}
