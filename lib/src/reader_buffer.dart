// Copyright (c) 2015, <your name>. All rights reserved. Use of this source code
// is governed by a BSD-style license that can be found in the LICENSE file.

library mysql_client.data_buffer;

import "package:mysql_client/src/data_chunk.dart";
import "package:mysql_client/src/data_range.dart";
import "package:mysql_client/src/data_commons.dart";
import 'dart:io';

class NullError extends Error {
  String toString() => "Null value";
}

class UndefinedError extends Error {
  String toString() => "Undefined value";
}

class EOFError extends Error {
  String toString() => "EOF value";
}

class ReaderBuffer {
  List<DataChunk> _chunks;
  int _payloadLength;

  int _chunkIndex;
  int _readCount;

  ReaderBuffer(this._chunks, this._payloadLength) {
    _chunkIndex = 0;
    _readCount = 0;
  }

  ReaderBuffer.reusable() : this._chunks = new List<DataChunk>();

  ReaderBuffer reuse(int reusableChunks, int payloadLength) {
    for (var i = reusableChunks; i < _chunks.length; i++) {
      _chunks[i].free();
    }
    _payloadLength = payloadLength;
    _chunkIndex = 0;
    _readCount = 0;
    return this;
  }

  void free() {
    for (var chunk in _chunks) {
      chunk.free();
    }
    _payloadLength = null;
    _chunkIndex = null;
    _readCount = null;
  }

  DataChunk getReusableChunk(int index) {
    if (_chunks.length > index) {
      return _chunks[index];
    } else {
      var chunk = new DataChunk.reusable();
      _chunks.add(chunk);
      return chunk;
    }
  }

  int get payloadLength => _payloadLength;

  int get available => _payloadLength - _readCount;

  bool get isAllRead => _payloadLength == _readCount;

  void skipByte() {
    _readOneByte();
  }

  void skipBytes(int length) {
    readFixedLengthDataRange(length, new DataRange.reusable());
  }

  int checkOneLengthInteger() => _chunks[_chunkIndex].checkOneByte();

  int readOneLengthInteger() => _readOneByte();

  int readFixedLengthInteger(int length) =>
      readFixedLengthDataRange(length, new DataRange.reusable()).toInt();

  int readLengthEncodedInteger() =>
      readLengthEncodedDataRange(new DataRange.reusable()).toInt();

  DataRange readNulTerminatedDataRange() =>
      readUpToDataRange(NULL_TERMINATOR, new DataRange.reusable());

  String readNulTerminatedString() =>
      readUpToDataRange(NULL_TERMINATOR, new DataRange.reusable()).toString();

  String readNulTerminatedUTF8String() =>
      readUpToDataRange(NULL_TERMINATOR, new DataRange.reusable())
          .toUTF8String();

  String readFixedLengthString(int length) =>
      readFixedLengthDataRange(length, new DataRange.reusable()).toString();

  String readFixedLengthUTF8String(int length) =>
      readFixedLengthDataRange(length, new DataRange.reusable()).toUTF8String();

  String readLengthEncodedString() =>
      readFixedLengthString(readLengthEncodedInteger());

  String readLengthEncodedUTF8String() =>
      readFixedLengthUTF8String(readLengthEncodedInteger());

  DataRange readRestOfPacketDataRange() => readFixedLengthDataRange(
      _payloadLength - _readCount, new DataRange.reusable());

  String readRestOfPacketString() =>
      readFixedLengthString(_payloadLength - _readCount);

  String readRestOfPacketUTF8String() =>
      readFixedLengthUTF8String(_payloadLength - _readCount);

  DataRange readFixedLengthDataRange(int length, DataRange reusableRange) {
    var chunk = _chunks[_chunkIndex];
    var range = chunk.extractFixedLengthDataRange(length, reusableRange);
    if (chunk.isEmpty) {
      _chunkIndex++;
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var data = new List(length);
      data.setRange(0, range.length, range.data, range.start);
      var start = range.length;
      var leftLength = length - range.length;
      do {
        chunk = _chunks[_chunkIndex];
        range = chunk.extractFixedLengthDataRange(leftLength, reusableRange);
        if (chunk.isEmpty) {
          _chunkIndex++;
        }
        var end = start + range.length;
        data.setRange(start, end, range.data, range.start);
        start = end;
        leftLength -= range.length;
      } while (range.isPending);

      range = reusableRange.reuse(data);
    }

    _readCount += range.length;

    return range;
  }

  DataRange readUpToDataRange(int terminator, DataRange reusableRange) {
    var chunk = _chunks[_chunkIndex];
    var range = chunk.extractUpToDataRange(terminator, reusableRange);
    if (chunk.isEmpty) {
      _chunkIndex++;
    }

    if (range.isPending) {
      // devo costruire un range da zero
      var builder = new BytesBuilder();
      builder.add(range.data.sublist(range.start, range.start + range.length));
      do {
        chunk = _chunks[_chunkIndex];
        range = chunk.extractUpToDataRange(terminator, reusableRange);
        if (chunk.isEmpty) {
          _chunkIndex++;
        }
        builder
            .add(range.data.sublist(range.start, range.start + range.length));
      } while (range.isPending);

      range = reusableRange.reuse(builder.takeBytes());
    }

    // skip the terminator
    _readCount += range.length + 1;
    return range;
  }

  DataRange readLengthEncodedDataRange(DataRange reusableRange) {
    var firstByte = _readOneByte();
    var bytesLength;
    switch (firstByte) {
      case PREFIX_INT_2:
        bytesLength = 3;
        break;
      case PREFIX_INT_3:
        bytesLength = 4;
        break;
      case PREFIX_INT_8:
        if (_payloadLength - _readCount >= 8) {
          bytesLength = 9;
        } else {
          throw new EOFError();
        }
        break;
      case PREFIX_NULL:
        throw new NullError();
      case PREFIX_UNDEFINED:
        throw new UndefinedError();
      default:
        return reusableRange.reuseByte(firstByte);
    }
    return readFixedLengthDataRange(bytesLength - 1, reusableRange);
  }

  int _readOneByte() {
    var chunk = _chunks[_chunkIndex];
    var byte = chunk.extractOneByte();
    if (chunk.isEmpty) {
      _chunkIndex++;
    }
    _readCount++;
    return byte;
  }
}
