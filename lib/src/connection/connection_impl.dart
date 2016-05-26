library mysql_client.connection.impl;

import 'dart:async';
import 'dart:io';
import 'dart:collection';

import "package:logging/logging.dart";

import '../protocol.dart';
import '../connection.dart';

typedef Future<CommandResult> Command();

class ConnectionFactoryImpl implements ConnectionFactory {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.ConnectionFactoryImpl");

  @override
  Future<Connection> connect(host, int port, String userName, String password,
      [String database]) async {
    var socket = await RawSocket.connect(host, port);
    socket.setOption(SocketOption.TCP_NODELAY, true);
    socket.writeEventsEnabled = false;

    var protocol = new Protocol(socket);

    try {
      var response =
          await protocol.connectionProtocol.readInitialHandshakeResponse();

      if (response is InitialHandshakePacket) {
        protocol.connectionProtocol.writeHandshakeResponsePacket(
            userName,
            password,
            database,
            response.authPluginData,
            response.authPluginName);

        response = await protocol.readCommandResponse();

        if (response is OkPacket) {
          return new ConnectionImpl(socket, protocol);
        } else {
          throw new ConnectionError(response.errorMessage);
        }
      } else {
        throw new ConnectionError(response.errorMessage);
      }
    } finally {
      protocol.connectionProtocol.free();
    }
  }
}

class ConnectionImpl extends ClosableImpl implements Connection {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.ConnectionImpl");

  RawSocket _socket;

  Protocol _protocol;

  Queue<CommandRequest> _requests = new Queue();

  ConnectionImpl(this._socket, this._protocol);

  @override
  Future<QueryResult> executeQuery(String query) {
    var request = requestQueryExecution(query);

    return request.response;
  }

  @override
  CommandRequest requestQueryExecution(String query) {
    return _requestCommandExecution(() async {
      try {
        _protocol.queryCommandTextProtocol.writeCommandQueryPacket(query);

        var response =
            await _protocol.queryCommandTextProtocol.readCommandQueryResponse();

        if (response is OkPacket) {
          return new CommandQueryResultImpl.ok(
              response.affectedRows, response.lastInsertId, this);
        } else if (response is ResultSetColumnCountPacket) {
          List<ColumnDefinition> columns = new List(response.columnCount);
          var columnIterator =
              new QueryColumnIteratorImpl(columns.length, this);
          var hasColumn = columns.length > 0;
          var i = 0;
          while (hasColumn) {
            hasColumn = await columnIterator.rawNext();
            if (hasColumn) {
              columns[i++] = new ColumnDefinition(
                  columnIterator.name, columnIterator.type);
            }
          }

          return new CommandQueryResultImpl.resultSet(columns, this);
        } else {
          throw new QueryError(response.errorMessage);
        }
      } finally {
        _protocol.queryCommandTextProtocol.free();
      }
    });
  }

  CommandRequest _requestCommandExecution(Command command) {
    _checkClosed();

    var request = new CommandRequestImpl();

/*
    // listen to close event
    request.onClosed.first.then((_) {
      _requests.removeFirst();

      // TODO avviso chi sta aspettando il suo turno
    });

    // add to the queue
    _requests.addLast(request);

    // TODO devo aspettare finchè la richiesta diventa la prima
    if (_requests.length > 1) {
      throw new UnsupportedError("Command request queuing not supported");
    }

    // a questo punto posso eseguire il comando
    var result = await request._executeCommand(command);
    return result;
*/
    return request;
  }

  Future _free() async {
    var requestToClose = new List.from(_requests);
    _requests.clear();
    var error;
    for (var request in requestToClose) {
      try {
        await request.close();
      } catch (e, s) {
        _logger.info("Error closing pending command request", e, s);

        error ??= e;
      }
    }

    if (error != null) {
      throw error;
    }
  }

  @override
  Future _closeInternal() async {
    try {
      // TODO gestione di uno stato di chiusura in corso (CLOSING)

      var error;
      try {
        await _free();
      } catch (e, s) {
        _logger.info("Error freeing connection", e, s);

        error ??= e;
      }

      // TODO chiudo tutti gli eventuali statement ancora aperti (senza inviare la richiesta di chiusura del protocollo)

      try {
        await _socket.close();
      } catch (e, s) {
        _logger.info("Error closing connection socket", e, s);

        error ??= e;
      }

      try {
        await super._closeInternal();
      } catch (e, s) {
        _logger.info("Error closing connection", e, s);

        error ??= e;
      }

      if (error != null) {
        throw error;
      }
    } finally {
      _socket = null;
      _protocol = null;
    }
  }
}

class CommandRequestImpl extends ClosableImpl implements CommandRequest {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.CommandRequestImpl");
/*
  Future<CommandResult> _executeCommand(Command command) async {
    try {
      var result = await command();

      if (result is ConsumableCommandResultImpl) {
        result.onClosed.first.then((_) {
          close();
        });
      } else {
        close();
      }

      return result;
    } catch (e) {
      close();

      rethrow;
    }
  }
*/

  Future<CommandResult> get response {
    throw new UnimplementedError();
  }
}

abstract class BaseQueryResultImpl extends ClosableImpl implements QueryResult {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.BaseQueryResultImpl");

  @override
  final int affectedRows;

  @override
  final int lastInsertId;

  RowIterator _rowIterator;

  BaseQueryResultImpl.resultSet()
      : this.affectedRows = null,
        this.lastInsertId = null {
    this._rowIterator = _createRowIterator();

    // TODO gestire l'ascolto della chiusura del row iterator con propagazione dell'evento
    // this._rowIterator.onClose;
  }

  // TODO gestire chiusura
  BaseQueryResultImpl.ok(this.affectedRows, this.lastInsertId)
      : this._rowIterator = null;

  RowIterator _createRowIterator();

  @override
  int get columnCount => columns?.length;

  // TODO gestire chiusura
  @override
  bool get isClosed => _rowIterator == null || _rowIterator.isClosed;

  @override
  Future<bool> next() => _rowIterator.next();

  @override
  rawNext() => _rowIterator.rawNext();

  @override
  String getStringValue(int index) => _rowIterator.getStringValue(index);

  @override
  num getNumValue(int index) => _rowIterator.getNumValue(index);

  @override
  bool getBoolValue(int index) => _rowIterator.getBoolValue(index);

  @override
  Future<List<List>> getNextRows() {
    // TODO implementare getNextRows
    throw new UnimplementedError();
  }

  // TODO gestire chiusura
  @override
  Future close() async {
    if (!isClosed) {
      await _rowIterator.close();
    }
  }
}

class CommandQueryResultImpl extends BaseQueryResultImpl {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.CommandQueryResultImpl");

  final ConnectionImpl _connection;

  @override
  final List<ColumnDefinition> columns;

  CommandQueryResultImpl.resultSet(this.columns, this._connection)
      : super.resultSet();

  CommandQueryResultImpl.ok(
      int affectedRows, int lastInsertId, this._connection)
      : this.columns = null,
        super.ok(affectedRows, lastInsertId);

  @override
  RowIterator _createRowIterator() => new CommandQueryRowIteratorImpl(this);

  Protocol get _protocol => _connection._protocol;
}

abstract class BaseDataIteratorImpl extends ClosableImpl
    implements DataIterator {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.BaseDataIteratorImpl");

  bool _isDataPacket(Packet packet);
  _readDataResponse();
  _skipDataResponse();
  _free();

  @override
  Future close() async {
    if (!_isClosed) {
      var hasNext = true;
      while (hasNext) {
        hasNext = _skip();
        hasNext = hasNext is Future ? await hasNext : hasNext;
      }

      // TODO gestire la notifica dell'evento di chiusura
      _isClosed = true;
    }
  }

  @override
  Future<bool> next() {
    var value = rawNext();
    return value is Future ? value : new Future.value(value);
  }

  @override
  rawNext() {
    _checkClosed();

    var response = _readDataResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  _skip() {
    var response = _skipDataResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  bool _checkNext(Packet packet) {
    if (_isDataPacket(packet)) {
      return true;
    } else {
      // TODO gestire la notifica dell'evento di chiusura
      _isClosed = true;
      _free();
      return false;
    }
  }
}

class QueryColumnIteratorImpl extends BaseDataIteratorImpl {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.QueryColumnIteratorImpl");

  final ConnectionImpl _connection;

  final int columnCount;

  QueryColumnIteratorImpl(this.columnCount, this._connection);

  String get catalog => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.catalog;
  String get schema => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.schema;
  String get table =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.table;
  String get orgTable => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.orgTable;
  String get name =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.name;
  String get orgName => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.orgName;
  int get fieldsLength => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.fieldsLength;
  int get characterSet => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.characterSet;
  int get columnLength => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.columnLength;
  int get type =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.type;
  int get flags =>
      _connection._protocol.queryCommandTextProtocol.reusableColumnPacket.flags;
  int get decimals => _connection
      ._protocol.queryCommandTextProtocol.reusableColumnPacket.decimals;

  @override
  bool _isDataPacket(Packet packet) =>
      packet is ResultSetColumnDefinitionPacket;

  @override
  _readDataResponse() => _connection._protocol.queryCommandTextProtocol
      .readResultSetColumnDefinitionResponse();

  @override
  _skipDataResponse() => _connection._protocol.queryCommandTextProtocol
      .skipResultSetColumnDefinitionResponse();

  @override
  _free() => _connection._protocol.queryCommandTextProtocol.free();
}

abstract class BaseQueryRowIteratorImpl<T extends QueryResult>
    extends BaseDataIteratorImpl implements RowIterator {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.BaseQueryRowIteratorImpl");

  final T _result;

  BaseQueryRowIteratorImpl(this._result);
}

class CommandQueryRowIteratorImpl extends BaseQueryRowIteratorImpl {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.CommandQueryRowIteratorImpl");

  CommandQueryRowIteratorImpl(CommandQueryResultImpl result) : super(result);

  @override
  String getStringValue(int index) =>
      _result._connection._protocol.queryCommandTextProtocol.reusableRowPacket
          .getUTF8String(index);

  @override
  num getNumValue(int index) {
    var formatted = _result
        ._connection._protocol.queryCommandTextProtocol.reusableRowPacket
        .getString(index);
    return formatted != null ? num.parse(formatted) : null;
  }

  @override
  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  @override
  bool _isDataPacket(Packet response) => response is ResultSetRowPacket;

  @override
  _readDataResponse() =>
      _result._protocol.queryCommandTextProtocol.readResultSetRowResponse();

  @override
  _skipDataResponse() =>
      _result._protocol.queryCommandTextProtocol.skipResultSetRowResponse();

  @override
  _free() => _result._connection._protocol.queryCommandTextProtocol.free();
}

abstract class ClosableImpl implements Closable {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.ClosableImpl");

  bool _isClosed;

  StreamController _onClosedController = new StreamController();

  ClosableImpl() {
    _isClosed = false;
  }

  @override
  bool get isClosed => _isClosed;

  Stream get onClosed => _onClosedController.stream;

  @override
  Future close() async {
    _checkClosed();

    await _closeInternal();

    _notifyClosed();
  }

  void _checkClosed() {
    if (isClosed) {
      throw new StateError("Already closed");
    }
  }

  Future _closeInternal() async {
    _isClosed = true;
  }

  void _notifyClosed() {
    _onClosedController.add(null);
  }
}
