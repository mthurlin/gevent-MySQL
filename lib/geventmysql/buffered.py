# Copyright (C) 2009, Hyves (Startphone Ltd.)
#
# This module is part of the Concurrence Framework and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php


from geventmysql._mysql import Buffer, BufferOverflowError, BufferUnderflowError, BufferInvalidArgumentError
from gevent import socket

class BufferedReader(object):
    def __init__(self, stream, buffer):
        assert stream is None or isinstance(stream, socket.socket)
        self.stream = stream
        self.buffer = buffer
        #assume no reading from underlying stream was done, so make sure buffer reflects this:
        self.buffer.position = 0
        self.buffer.limit = 0

    #def file(self):
    #    return CompatibleFile(self, None)

    def clear(self):
        self.buffer.clear()

    def _read_more(self):
        #any partially read data will be put in front, otherwise normal clear:
        self.buffer.compact()
        data = self.stream.recv(self.buffer.limit - self.buffer.position)
        if not data:   
            raise EOFError("while reading")
        self.buffer.write_bytes(data)
        self.buffer.flip() #prepare to read from buffer

    def read_lines(self):
        """note that it cant read line accross buffer"""
        if self.buffer.remaining == 0:
            self._read_more()
        while True:
            try:
                yield self.buffer.read_line()
            except BufferUnderflowError:
                self._read_more()

    def read_line(self):
        """note that it cant read line accross buffer"""
        if self.buffer.remaining == 0:
            self._read_more()
        while True:
            try:
                return self.buffer.read_line()
            except BufferUnderflowError:
                self._read_more()

    def read_bytes_available(self):
        if self.buffer.remaining == 0:
            self._read_more()
        return self.buffer.read_bytes(-1)

    def read_bytes(self, n):
        """read exactly n bytes from stream"""
        buffer = self.buffer
        s = []
        while n > 0:
            r = buffer.remaining
            if r > 0:
                s.append(buffer.read_bytes(min(n, r)))
                n -= r
            else:
                self._read_more()

        return ''.join(s)

    def read_int(self):
        if self.buffer.remaining == 0:
            self._read_more()
        while True:
            try:
                return self.buffer.read_int()
            except BufferUnderflowError:
                self._read_more()

    def read_short(self):
        if self.buffer.remaining == 0:
            self._read_more()
        while True:
            try:
                return self.buffer.read_short()
            except BufferUnderflowError:
                self._read_more()

class BufferedWriter(object):
    def __init__(self, stream, buffer):
        assert stream is None or isinstance(stream, socket.socket)
        self.stream = stream
        self.buffer = buffer

    #def file(self):
    #    return CompatibleFile(None, self)

    def clear(self):
        self.buffer.clear()

    def write_bytes(self, s):
        assert type(s) == str, "arg must be a str, got: %s" % type(s)
        try:
            self.buffer.write_bytes(s)
        except BufferOverflowError:
            #we need to send it in parts, flushing as we go
            while s:
                r = self.buffer.remaining
                part, s = s[:r], s[r:]
                self.buffer.write_bytes(part)
                self.flush()

    def write_byte(self, ch):
        assert type(ch) == int, "ch arg must be int"
        while True:
            try:
                self.buffer.write_byte(ch)
                return
            except BufferOverflowError:
                self.flush()

    def write_short(self, i):
        while True:
            try:
                self.buffer.write_short(i)
                return
            except BufferOverflowError:
                self.flush()

    def write_int(self, i):
        while True:
            try:
                self.buffer.write_int(i)
                return
            except BufferOverflowError:
                self.flush()

    def flush(self):
        self.buffer.flip()
        bytes = self.buffer.read_bytes()
        self.stream.sendall(bytes)
        self.buffer.clear()

class BufferedStream(object):

    _reader_pool = {} #buffer_size -> [list of readers]
    _writer_pool = {} #bufffer_size -> [list of writers]

    __slots__ = ['_stream', '_writer', '_reader', '_read_buffer_size', '_write_buffer_size']

    def __init__(self, stream, buffer_size = 1024 * 8, read_buffer_size = 0, write_buffer_size = 0):
        self._stream = stream
        self._writer = None
        self._reader = None
        self._read_buffer_size = read_buffer_size or buffer_size
        self._write_buffer_size = write_buffer_size or buffer_size

    def flush(self):
        if self._writer:
            self._writer.flush()

    @property
    def reader(self):
        if self._reader is None:
            self._reader = BufferedReader(self._stream, Buffer(self._read_buffer_size))
        return self._reader

    @property
    def writer(self):
        if self._writer is None:
            self._writer = BufferedWriter(self._stream, Buffer(self._write_buffer_size))
        return self._writer

    class _borrowed_writer(object):
        def __init__(self, stream):
            buffer_size = stream._write_buffer_size
            if stream._writer is None:
                if stream._writer_pool.get(buffer_size, []):
                    writer = stream._writer_pool[buffer_size].pop()
                else:
                    writer = BufferedWriter(None, Buffer(buffer_size))
            else:
                writer = stream._writer
            writer.stream = stream._stream
            self._writer = writer
            self._stream = stream

        def __enter__(self):
            return self._writer

        def __exit__(self, type, value, traceback):
            #TODO!!! handle exception case/exit
            if self._writer.buffer.position != 0:
                self._stream._writer = self._writer
            else:
                writer_pool = self._stream._writer_pool.setdefault(self._stream._write_buffer_size, [])
                writer_pool.append(self._writer)
                self._stream._writer = None

    class _borrowed_reader(object):
        def __init__(self, stream):
            buffer_size = stream._read_buffer_size
            if stream._reader is None:
                if stream._reader_pool.get(buffer_size, []):
                    reader = stream._reader_pool[buffer_size].pop()
                else:
                    reader = BufferedReader(None, Buffer(buffer_size))
            else:
                reader = stream._reader
            reader.stream = stream._stream
            self._reader = reader
            self._stream = stream

        def __enter__(self):
            return self._reader

        def __exit__(self, type, value, traceback):
            #TODO!!! handle exception case/exit
            if self._reader.buffer.remaining:
                self._stream._reader = self._reader
            else:
                reader_pool = self._stream._reader_pool.setdefault(self._stream._read_buffer_size, [])
                reader_pool.append(self._reader)
                self._stream._reader = None

    def get_writer(self):
        return self._borrowed_writer(self)

    def get_reader(self):
        return self._borrowed_reader(self)

    def close(self):
        self._stream.close()
        del self._stream
        del self._reader
        del self._writer
'''
class CompatibleFile(object):
    """A wrapper that implements python's file like object semantics on top
    of concurrence BufferedReader and or BufferedWriter. Don't create
    this object directly, but use the file() method on BufferedReader or BufferedWriter"""
    def __init__(self, reader = None, writer = None):
        self._reader = reader
        self._writer = writer

    def readlines(self):
        reader = self._reader
        buffer = reader.buffer
        while True:
            try:
                yield buffer.read_line(True)
            except BufferUnderflowError:
                try:
                    reader._read_more()
                except EOFError:
                    buffer.flip()
                    yield buffer.read_bytes(-1)

    def readline(self):
        return self.readlines().next()

    def read(self, n = -1):
        reader = self._reader
        buffer = reader.buffer
        s = []
        if n == -1: #read all available bytes until EOF
            while True:
                s.append(buffer.read_bytes(-1))
                try:
                    reader._read_more()
                except EOFError:
                    buffer.flip()
                    break
        else:
            while n > 0: #read uptill n avaiable bytes or EOF
                r = buffer.remaining
                if r > 0:
                    s.append(buffer.read_bytes(min(n, r)))
                    n -= r
                else:
                    try:
                        reader._read_more()
                    except EOFError:
                        buffer.flip()
                        break
        return ''.join(s)

    def write(self, s):
        self._writer.write_bytes(s)

    def flush(self):
        self._writer.flush()


'''