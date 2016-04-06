library mysql_client.connection.impl;

import 'dart:async';
import 'dart:io';
import 'dart:collection';

import "package:logging/logging.dart";
import 'package:pool/pool.dart';

import '../protocol.dart';
import '../connection.dart';

typedef Future<CommandResult> Command();

// TODO statement
/*
int _getSqlType(SqlType sqlType) {
  switch (sqlType) {
    case SqlType.DATETIME:
      return MYSQL_TYPE_DATETIME;
    case SqlType.DOUBLE:
      return MYSQL_TYPE_DOUBLE;
    case SqlType.LONG:
      return MYSQL_TYPE_LONG;
    case SqlType.LONGLONG:
      return MYSQL_TYPE_LONGLONG;
    case SqlType.NULL:
      return MYSQL_TYPE_NULL;
    case SqlType.TIMESTAMP:
      return MYSQL_TYPE_TIMESTAMP;
    case SqlType.TINY:
      return MYSQL_TYPE_TINY;
    case SqlType.VAR_STRING:
      return MYSQL_TYPE_VAR_STRING;
  }
}
*/
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

class ConnectionPoolImpl extends ClosableImpl implements ConnectionPool {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.ConnectionPoolImpl");

  final ConnectionFactory _factory;
  final Pool _pool;
  final _host;
  final int _port;
  final String _userName;
  final String _password;
  final String _database;

  final Queue<Connection> _releasedConnections = new Queue();
  final Map<PooledConnectionImpl, PoolResource> _assignedResources = new Map();
  final Map<PooledConnectionImpl, ConnectionImpl> _assignedConnections =
      new Map();

  ConnectionPoolImpl(
      {host,
      int port,
      String userName,
      String password,
      String database,
      int maxConnections,
      Duration connectionTimeout})
      : this._host = host,
        this._port = port,
        this._userName = userName,
        this._password = password,
        this._database = database,
        this._factory = new ConnectionFactory(),
        this._pool = new Pool(maxConnections, timeout: connectionTimeout);

  @override
  Future<Connection> request() async {
    _checkClosed();

    var resource = await _pool.request();

    var connection = _releasedConnections.isNotEmpty
        ? _releasedConnections.removeLast()
        : null;

    if (connection == null) {
      try {
        connection = await _factory.connect(
            _host, _port, _userName, _password, _database);
      } catch (e) {
        resource.release();

        rethrow;
      }
    }

    var pooledConnection = new PooledConnectionImpl(connection, this);

    _assignedConnections[pooledConnection] = connection;
    _assignedResources[pooledConnection] = resource;

    return pooledConnection;
  }

  @override
  Future _closeInternal() async {
    try {
      var error;

      try {
        await Future.wait(_assignedConnections.keys
            .map((pooledConnection) => _release(pooledConnection)));
      } catch (e, s) {
        _logger.info("Error releasing busy connections", e, s);

        error ??= e;
      }

      try {
        await _pool.close();
      } catch (e, s) {
        _logger.info("Error closing connection pool", e, s);

        error ??= e;
      }

      try {
        await Future
            .wait(_releasedConnections.map((connection) => connection.close()));
      } catch (e, s) {
        _logger.info("Error closing connections", e, s);

        error ??= e;
      }

      try {
        await super._closeInternal();
      } catch (e, s) {
        _logger.info("Error closing connection pool", e, s);

        error ??= e;
      }

      if (error != null) {
        throw error;
      }
    } finally {
      _releasedConnections.clear();
    }
  }

  Future _release(PooledConnectionImpl pooledConnection) async {
    var connection = _assignedConnections.remove(pooledConnection);
    var resource = _assignedResources.remove(pooledConnection);

    try {
      await connection._free();
    } finally {
      resource.release();

      _releasedConnections.add(connection);
    }
  }
}

class PooledConnectionImpl extends ClosableImpl implements Connection {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.PooledConnectionImpl");

  ConnectionPoolImpl _connectionPool;

  ConnectionImpl _connection;

  PooledConnectionImpl(this._connection, this._connectionPool);

  @override
  Future<QueryResult> executeQuery(String query) {
    _checkClosed();

    return _connection.executeQuery(query);
  }

  // TODO statement
/*
  @override
  Future<PreparedStatement> prepareQuery(String query) {
    _checkClosed();

    return _connection.prepareQuery(query);
  }
*/
  @override
  Future _closeInternal() async {
    try {
      var error;

      try {
        await _connectionPool._release(this);
      } catch (e, s) {
        _logger.info("Error releasing pooled connection", e, s);

        error ??= e;
      }

      try {
        await super._closeInternal();
      } catch (e, s) {
        _logger.info("Error closing pooled connection", e, s);

        error ??= e;
      }

      if (error != null) {
        throw error;
      }
    } finally {
      _connection = null;
      _connectionPool = null;
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

  Future<QueryResult> executeQuery(String query) async {
    var request = await requestQueryExecution(query);

    return await request.response;
  }

  Future<CommandRequest> requestQueryExecution(String query) async {
    _checkClosed();

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

  @override
  Future<QueryResult> executeQuery(String query) async {
    _checkClosed();

    return await _reserveCommandExecution(() async {
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

  // TODO statement
/*
  @override
  Future<PreparedStatement> prepareQuery(String query) async {
    _checkClosed();

    var request = new CommandRequest();

    try {
      _protocol.preparedStatementProtocol
          .writeCommandStatementPreparePacket(query);

      await _waitForExecutionReady(request);

      var response = await _protocol.preparedStatementProtocol
          .readCommandStatementPrepareResponse();

      if (response is CommandStatementPrepareOkResponsePacket) {
        List<ColumnDefinition> parameters = new List(response.numParams);
        var parameterIterator =
            new QueryColumnIteratorImpl(parameters.length, this);
        var hasParameter = parameters.length > 0;
        var i = 0;
        while (hasParameter) {
          hasParameter = await parameterIterator.rawNext();
          if (hasParameter) {
            parameters[i++] = new ColumnDefinition(
                parameterIterator.name, parameterIterator.type);
          }
        }

        List<ColumnDefinition> columns = new List(response.numColumns);
        var columnIterator = new QueryColumnIteratorImpl(columns.length, this);
        var hasColumn = columns.length > 0;
        var l = 0;
        while (hasColumn) {
          hasColumn = await columnIterator.rawNext();
          if (hasColumn) {
            columns[l++] =
                new ColumnDefinition(columnIterator.name, columnIterator.type);
          }
        }

        request._addResult(new PreparedStatementImpl(
            response.statementId, parameters, columns, this));
      } else {
        throw new PreparedStatementError(response.errorMessage);
      }
    } finally {
      _protocol.preparedStatementProtocol.free();

      // TODO in realtà una volta arrivati qui la richiesta si può già chiudere
    }

    return request._result;
  }
*/

  Future _free() async {
    // TODO chiusura delle richieste pending (forse meglio chiamarlo abort)
    try {
      var requestToClose = new List.from(_requests);
      for (var request in requestToClose) {
        await request.close();
      }
    } finally {
      _requests.clear();
    }
  }

  @override
  Future _closeInternal() async {
    try {
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

  Future<CommandResult> _reserveCommandExecution(Command command) async {
    var request = new CommandRequest();

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
  }
}

class CommandRequest extends ClosableImpl {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.CommandRequest");

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

// TODO statement
/*
class PreparedStatementImpl implements PreparedStatement {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.PreparedStatementImpl");

  final ConnectionImpl _connection;

  final int _statementId;

  @override
  final List<ColumnDefinition> parameters;

  @override
  final List<ColumnDefinition> columns;

  final List<int> _parameterTypes;
  final List _parameterValues;
  List<int> _columnTypes;

  bool _isClosed;
  bool _isNewParamsBoundFlag;

  PreparedStatementImpl(this._statementId, List<ColumnDefinition> parameters,
      List<ColumnDefinition> columns, this._connection)
      : this.parameters = parameters,
        this.columns = columns,
        this._parameterTypes = new List(parameters.length),
        this._parameterValues = new List(parameters.length) {
    _isClosed = false;
    _isNewParamsBoundFlag = true;
    _columnTypes = new List.generate(
        columns.length, (index) => columns[index].type,
        growable: false);
  }

  @override
  int get parameterCount => parameters.length;

  @override
  int get columnCount => columns.length;

  @override
  bool get isClosed => _isClosed;

  @override
  void setParameter(int index, value, [SqlType sqlType]) {
    _checkClosed();

    if (index >= parameterCount) {
      throw new IndexError(index, _parameterValues);
    }

    var type = _getSqlType(sqlType);

    type ??= _connection._protocol.preparedStatementProtocol
        .getSqlTypeFromValue(value);

    if (type != null && _parameterTypes[index] != type) {
      _parameterTypes[index] = type;
      _isNewParamsBoundFlag = true;
    }

    _parameterValues[index] = value;
  }

  @override
  Future<QueryResult> executeQuery() async {
    _checkClosed();

    var request = new CommandRequest();

    try {
      _connection._protocol.preparedStatementProtocol
          .writeCommandStatementExecutePacket(_statementId, _parameterValues,
              _isNewParamsBoundFlag, _parameterTypes);

      await _connection._waitForExecutionReady(request);

      _isNewParamsBoundFlag = false;

      var response = await _connection._protocol.preparedStatementProtocol
          .readCommandStatementExecuteResponse();

      if (response is OkPacket) {
        request._addResult(new PreparedQueryResultImpl.ok(
            response.affectedRows, response.lastInsertId));
      } else if (response is ResultSetColumnCountPacket) {
        // TODO capire se c'è la possibilità di skippare più velocemente le colonne

        var columnIterator =
            new QueryColumnIteratorImpl(columnCount, _connection);
        var hasColumn = true;
        while (hasColumn) {
          hasColumn = await columnIterator._skip();
        }

        request._addResult(new PreparedQueryResultImpl.resultSet(this));
      } else {
        throw new QueryError(response.errorMessage);
      }
    } finally {
      _connection._protocol.preparedStatementProtocol.free();
    }

    return request._result;
  }

  @override
  Future close() async {
    _checkClosed();

    try {
      var error;

      try {
        await super.close();
      } catch (e, s) {
        _logger.info("Error closing prepared statement", e, s);

        error ??= e;
      }

      try {
        _connection._protocol.preparedStatementProtocol
            .writeCommandStatementClosePacket(_statementId);
      } catch (e, s) {
        _logger.info("Error closing prepared statement", e, s);

        error ??= e;
      } finally {
        _connection._protocol.preparedStatementProtocol.free();
      }

      if (error != null) {
        throw error;
      }
    } finally {
      _isClosed = true;
    }
  }

  Future _free() async {
    // TODO non posso chiudere lo statement ma posso liberare qualcosa?
  }

  void _checkClosed() {
    if (_isClosed) {
      throw new StateError("Prepared statement closed");
    }
  }
}

class PreparedQueryResultImpl extends BaseQueryResultImpl {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.PreparedQueryResultImpl");

  final PreparedStatement _statement;

  PreparedQueryResultImpl.resultSet(PreparedStatement statement)
      : this._statement = statement,
        super.resultSet();

  PreparedQueryResultImpl.ok(int affectedRows, int lastInsertId)
      : this._statement = null,
        super.ok(affectedRows, lastInsertId);

  @override
  RowIterator _createRowIterator() => new PreparedQueryRowIteratorImpl(this);

  @override
  List<ColumnDefinition> get columns => _statement?.columns;
}
*/

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

// TODO statement
/*
class PreparedQueryRowIteratorImpl extends BaseQueryRowIteratorImpl {
  static final Logger _logger =
      new Logger("mysql_client.connection.impl.PreparedQueryRowIteratorImpl");

  PreparedQueryRowIteratorImpl(PreparedQueryResultImpl result) : super(result);

  @override
  String getStringValue(int index) => _result._statement._connection._protocol
      .preparedStatementProtocol.reusableRowPacket.getUTF8String(index);

  @override
  num getNumValue(int index) {
    var column = _result._statement.columns[index];
    switch (column.type) {
      case MYSQL_TYPE_TINY:
      case MYSQL_TYPE_LONG:
      case MYSQL_TYPE_LONGLONG:
        return _result._statement._connection._protocol
            .preparedStatementProtocol.reusableRowPacket.getInteger(index);
      case MYSQL_TYPE_DOUBLE:
        return _result._statement._connection._protocol
            .preparedStatementProtocol.reusableRowPacket.getDouble(index);
      default:
        throw new UnsupportedError("Sql type not supported ${column.type}");
    }
  }

  @override
  bool getBoolValue(int index) {
    var formatted = getNumValue(index);
    return formatted != null ? formatted != 0 : null;
  }

  @override
  _skip() {
    var response = _result._statement._connection._protocol
        .preparedStatementProtocol.skipResultSetRowResponse();

    return response is Future
        ? response.then((response) => _checkNext(response))
        : _checkNext(response);
  }

  @override
  bool _checkNext(Packet response) {
    if (response is PreparedResultSetRowPacket) {
      return true;
    } else {
      _isClosed = true;
      _result._statement._connection._protocol.preparedStatementProtocol.free();
      return false;
    }
  }

  @override
  bool _isDataPacket(Packet response) => response is PreparedResultSetRowPacket;

  @override
  _readDataResponse() =>
      _result._statement._connection._protocol.preparedStatementProtocol
          .readResultSetRowResponse(_result._statement._columnTypes);

  @override
  _skipDataResponse() =>
      _result._protocol.preparedStatementProtocol.skipResultSetRowResponse();

  @override
  _free() => _result._connection._protocol.preparedStatementProtocol.free();
}
*/

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
