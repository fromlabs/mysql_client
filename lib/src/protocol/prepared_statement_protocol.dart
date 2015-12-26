part of mysql_client.protocol;

class PrepareStatementError extends Error {}

class PreparedStatementProtocol extends Protocol {
  QueryCommandTextProtocol _queryCommandTextProtocol;

  PreparedStatementProtocol(DataWriter writer, DataReader reader,
      int serverCapabilityFlags, int clientCapabilityFlags)
      : super(writer, reader, serverCapabilityFlags, clientCapabilityFlags) {
    _queryCommandTextProtocol = new QueryCommandTextProtocol(
        writer, reader, serverCapabilityFlags, clientCapabilityFlags);
  }

  Future<PreparedStatement> prepareQuery(String query) async {
    _writeCommandStatementPreparePacket(query);

    // First packet:
    var response = await _readCommandStatementPrepareResponse();

    if (response is! CommandStatementPrepareOkResponsePacket) {
      throw new PrepareStatementError();
    }

    return new PreparedStatement(
        response.statementId, response.numColumns, response.numParams, this);
  }

  Future<Packet> _readCommandStatementPrepareResponse() => _readPacketBuffer()
      .thenFuture((_) => _readCommandStatementPrepareResponseInternal());

  _readResultSetColumnDefinitionResponse(
          ResultSetColumnDefinitionResponsePacket reusablePacket) =>
      _queryCommandTextProtocol
          ._readResultSetColumnDefinitionResponse(reusablePacket);

  Packet _readCommandStatementPrepareResponseInternal() {
    if (_isErrorPacket()) {
      return _readErrorPacket();
    } else {
      return _readCommandStatementPrepareOkResponsePacket();
    }
  }

  CommandStatementPrepareOkResponsePacket _readCommandStatementPrepareOkResponsePacket() {
    var packet = new CommandStatementPrepareOkResponsePacket(
        _reusablePacketBuffer.sequenceId, _reusablePacketBuffer.payloadLength);

    // status (1) -- [00] OK
    packet._status = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange)
        .toInt();
    // statement_id (4) -- statement-id
    packet._statementId = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(4, _reusableDataRange)
        .toInt();
    // num_columns (2) -- number of columns
    packet._numColumns = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _reusableDataRange)
        .toInt();
    // num_params (2) -- number of params
    packet._numParams = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _reusableDataRange)
        .toInt();
    // reserved_1 (1) -- [00] filler
    _reusablePacketBuffer.payload
        .readFixedLengthDataRange(1, _reusableDataRange);
    // warning_count (2) -- number of warnings
    packet._warningCount = _reusablePacketBuffer.payload
        .readFixedLengthDataRange(2, _reusableDataRange)
        .toInt();

    _reusablePacketBuffer.free();
    _reusableDataRange.free();

    return packet;
  }

  void _writeCommandStatementPreparePacket(String query) {
    WriterBuffer buffer = _writer.createBuffer();

    var sequenceId = 0x00;

    // command (1) -- [16] the COM_STMT_PREPARE command
    buffer.writeFixedLengthInteger(COM_STMT_PREPARE, 1);
    // query (string.EOF) -- the query to prepare
    buffer.writeFixedLengthUTF8String(query);

    var headerBuffer = _writer.createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _writer.writeBuffer(headerBuffer);
    _writer.writeBuffer(buffer);
  }

  void _writeCommandStatementClosePacket(int statementId) {
    WriterBuffer buffer = _writer.createBuffer();

    var sequenceId = 0x00;

    // 1              [19] COM_STMT_CLOSE
    buffer.writeFixedLengthInteger(COM_STMT_CLOSE, 1);
    // 4              statement-id
    buffer.writeFixedLengthInteger(statementId, 4);

    var headerBuffer = _writer.createBuffer();
    headerBuffer.writeFixedLengthInteger(buffer.length, 3);
    headerBuffer.writeOneLengthInteger(sequenceId);

    _writer.writeBuffer(headerBuffer);
    _writer.writeBuffer(buffer);
  }
}

class PreparedStatement {
  final int _statementId;
  final int _numColumns;
  final int _numParams;

  final PreparedStatementProtocol _protocol;

  StatementColumnSetReader _paramSetReader;

  StatementColumnSetReader _columnSetReader;

  PreparedStatement(
      this._statementId, this._numColumns, this._numParams, this._protocol);

  StatementColumnSetReader get paramSetReader {
    // TODO check dello stato

    _paramSetReader = new StatementColumnSetReader(_numParams, _protocol);

    return _paramSetReader;
  }

  StatementColumnSetReader get columnSetReader {
    // TODO check dello stato

    _columnSetReader = new StatementColumnSetReader(_numColumns, _protocol);

    return _columnSetReader;
  }

  void close() {
    _protocol._writeCommandStatementClosePacket(_statementId);
  }
}

class StatementColumnSetReader extends SetReader {
  final int _columnCount;

  final PreparedStatementProtocol _protocol;

  final ResultSetColumnDefinitionResponsePacket _reusableColumnPacket;

  StatementColumnSetReader(this._columnCount, this._protocol)
      : this._reusableColumnPacket =
            new ResultSetColumnDefinitionResponsePacket.reusable();

  Future<bool> next() {
    var value = _columnCount > 0 ? internalNext() : false;
    return value is Future ? value : new Future.value(value);
  }

  internalNext() {
    // TODO check dello stato

    var response =
        _protocol._readResultSetColumnDefinitionResponse(_reusableColumnPacket);

    return response is Future
        ? response.then(
            (response) => response is ResultSetColumnDefinitionResponsePacket)
        : response is ResultSetColumnDefinitionResponsePacket;
  }

  String get name => _reusableColumnPacket.orgName;

  void close() {
    // TODO check dello stato

    _reusableColumnPacket.free();
  }
}

class CommandStatementPrepareOkResponsePacket extends Packet {
  int _status;
  int _statementId;
  int _numColumns;
  int _numParams;
  int _warningCount;

  CommandStatementPrepareOkResponsePacket(int payloadLength, int sequenceId)
      : super(payloadLength, sequenceId);

  int get status => _status;
  int get statementId => _statementId;
  int get numColumns => _numColumns;
  int get numParams => _numParams;
  int get warningCount => _warningCount;
}
