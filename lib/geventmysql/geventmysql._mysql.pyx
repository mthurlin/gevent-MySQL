# Copyright (C) 2009, Hyves (Startphone Ltd.)
#
# This module is part of the Concurrence Framework and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php

"""
base aynchronous mysql io library
"""


import datetime
import types
import sys

cdef extern from "string.h":
    cdef void *memmove(void *, void *, int)
    cdef void *memcpy(void *, void *, int)
    cdef void *memchr(void *, int, int)
     
cdef extern from "stdlib.h":
    cdef void *calloc(int, int)
    cdef void free(void *)    


cdef extern from "Python.h":
    object PyString_FromStringAndSize(char *, int)
    object PyString_FromString(char *)
    int PyString_AsStringAndSize(object obj, char **s, Py_ssize_t *len) except -1


cdef enum:
    COMMAND_SLEEP = 0
    COMMAND_QUIT  = 1
    COMMAND_INIT_DB = 2
    COMMAND_QUERY = 3
    COMMAND_LIST = 4

class COMMAND:
    SLEEP = COMMAND_SLEEP
    QUIT = COMMAND_QUIT
    INIT_DB = COMMAND_INIT_DB
    QUERY = COMMAND_QUERY
    LIST = COMMAND_LIST    
    
cdef enum:
    PACKET_READ_NONE =  0
    PACKET_READ_MORE =  1
    PACKET_READ_ERROR = 2                 
    PACKET_READ_TRUE =  4                    
    PACKET_READ_START = 8
    PACKET_READ_END =   16
    PACKET_READ_EOF =   32

class PACKET_READ_RESULT:
    NONE = PACKET_READ_NONE
    MORE = PACKET_READ_MORE
    ERROR = PACKET_READ_ERROR
    TRUE = PACKET_READ_TRUE
    START = PACKET_READ_START
    END = PACKET_READ_END
    EOF = PACKET_READ_EOF

cdef enum:
    FIELD_TYPE_DECIMAL = 0x00
    FIELD_TYPE_TINY = 0x01
    FIELD_TYPE_SHORT = 0x02
    FIELD_TYPE_LONG = 0x03
    FIELD_TYPE_FLOAT = 0x04
    FIELD_TYPE_DOUBLE = 0x05
    FIELD_TYPE_NULL = 0x06
    FIELD_TYPE_TIMESTAMP = 0x07
    FIELD_TYPE_LONGLONG = 0x08
    FIELD_TYPE_INT24 = 0x09
    FIELD_TYPE_DATE = 0x0a
    FIELD_TYPE_TIME = 0x0b
    FIELD_TYPE_DATETIME = 0x0c
    FIELD_TYPE_YEAR = 0x0d
    FIELD_TYPE_NEWDATE = 0x0e
    FIELD_TYPE_VARCHAR = 0x0f
    FIELD_TYPE_BIT = 0x10
    FIELD_TYPE_NEWDECIMAL = 0xf6
    FIELD_TYPE_ENUM = 0xf7
    FIELD_TYPE_SET = 0xf8
    FIELD_TYPE_TINY_BLOB = 0xf9
    FIELD_TYPE_MEDIUM_BLOB = 0xfa
    FIELD_TYPE_LONG_BLOB = 0xfb
    FIELD_TYPE_BLOB = 0xfc
    FIELD_TYPE_VAR_STRING = 0xfd
    FIELD_TYPE_STRING = 0xfe
    FIELD_TYPE_GEOMETRY = 0xff

class FIELD_TYPE:
    DECIMAL = FIELD_TYPE_DECIMAL
    TINY = FIELD_TYPE_TINY
    SHORT = FIELD_TYPE_SHORT
    LONG = FIELD_TYPE_LONG
    FLOAT = FIELD_TYPE_FLOAT
    DOUBLE = FIELD_TYPE_DOUBLE
    _NULL = FIELD_TYPE_NULL
    TIMESTAMP = FIELD_TYPE_TIMESTAMP
    LONGLONG = FIELD_TYPE_LONGLONG
    INT24 = FIELD_TYPE_INT24
    DATE = FIELD_TYPE_DATE
    TIME = FIELD_TYPE_TIME
    DATETIME = FIELD_TYPE_DATETIME
    YEAR = FIELD_TYPE_YEAR
    NEWDATE = FIELD_TYPE_NEWDATE
    VARCHAR = FIELD_TYPE_VARCHAR
    BIT = FIELD_TYPE_BIT
    NEWDECIMAL = FIELD_TYPE_NEWDECIMAL
    ENUM = FIELD_TYPE_ENUM
    SET = FIELD_TYPE_SET
    TINY_BLOB = FIELD_TYPE_TINY_BLOB
    MEDIUM_BLOB = FIELD_TYPE_MEDIUM_BLOB
    LONG_BLOB = FIELD_TYPE_LONG_BLOB
    BLOB = FIELD_TYPE_BLOB 
    VAR_STRING = FIELD_TYPE_VAR_STRING
    STRING = FIELD_TYPE_STRING
    GEOMETRY = FIELD_TYPE_GEOMETRY


INT_TYPES = set([FIELD_TYPE.TINY, FIELD_TYPE.SHORT, FIELD_TYPE.LONG, FIELD_TYPE.LONGLONG])
FLOAT_TYPES = set([FIELD_TYPE.FLOAT, FIELD_TYPE.DOUBLE])
BLOB_TYPES = set([FIELD_TYPE.TINY_BLOB, FIELD_TYPE.MEDIUM_BLOB, FIELD_TYPE.LONG_BLOB, FIELD_TYPE.BLOB])
STRING_TYPES = set([FIELD_TYPE.VARCHAR, FIELD_TYPE.VAR_STRING, FIELD_TYPE.STRING])
DATE_TYPES = set([FIELD_TYPE.TIMESTAMP, FIELD_TYPE.DATE, FIELD_TYPE.TIME, FIELD_TYPE.DATETIME, FIELD_TYPE.YEAR, FIELD_TYPE.NEWDATE])

# Not handled:
# 0x00 FIELD_TYPE_DECIMAL
# 0x06 FIELD_TYPE_NULL
# 0x09 FIELD_TYPE_INT24
# 0x10 FIELD_TYPE_BIT
# 0xf6 FIELD_TYPE_NEWDECIMAL
# 0xf7 FIELD_TYPE_ENUM
# 0xf8 FIELD_TYPE_SET
# 0xff FIELD_TYPE_GEOMETRY

charset_nr = {}
charset_nr[1] = 'big5'
charset_nr[2] = 'latin2'
charset_nr[3] = 'dec8'
charset_nr[4] = 'cp850'
charset_nr[5] = 'latin1'
charset_nr[6] = 'hp8'
charset_nr[7] = 'koi8r'
charset_nr[8] = 'latin1'
charset_nr[9] = 'latin2'
charset_nr[10] = 'swe7'
charset_nr[11] = 'ascii'
charset_nr[12] = 'ujis'
charset_nr[13] = 'sjis'
charset_nr[14] = 'cp1251'
charset_nr[15] = 'latin1'
charset_nr[16] = 'hebrew'
charset_nr[18] = 'tis620'
charset_nr[19] = 'euckr'
charset_nr[20] = 'latin7'
charset_nr[21] = 'latin2'
charset_nr[22] = 'koi8u'
charset_nr[23] = 'cp1251'
charset_nr[24] = 'gb2312'
charset_nr[25] = 'greek'
charset_nr[26] = 'cp1250'
charset_nr[27] = 'latin2'
charset_nr[28] = 'gbk'
charset_nr[29] = 'cp1257'
charset_nr[30] = 'latin5'
charset_nr[31] = 'latin1'
charset_nr[32] = 'armscii8'
charset_nr[33] = 'utf8'
charset_nr[34] = 'cp1250'
charset_nr[35] = 'ucs2'
charset_nr[36] = 'cp866'
charset_nr[37] = 'keybcs2'
charset_nr[38] = 'macce'
charset_nr[39] = 'macroman'
charset_nr[40] = 'cp852'
charset_nr[41] = 'latin7'
charset_nr[42] = 'latin7'
charset_nr[43] = 'macce'
charset_nr[44] = 'cp1250'
charset_nr[47] = 'latin1'
charset_nr[48] = 'latin1'
charset_nr[49] = 'latin1'
charset_nr[50] = 'cp1251'
charset_nr[51] = 'cp1251'
charset_nr[52] = 'cp1251'
charset_nr[53] = 'macroman'
charset_nr[57] = 'cp1256'
charset_nr[58] = 'cp1257'
charset_nr[59] = 'cp1257'
charset_nr[63] = 'binary'
charset_nr[64] = 'armscii8'
charset_nr[65] = 'ascii'
charset_nr[66] = 'cp1250'
charset_nr[67] = 'cp1256'
charset_nr[68] = 'cp866'
charset_nr[69] = 'dec8'
charset_nr[70] = 'greek'
charset_nr[71] = 'hebrew'
charset_nr[72] = 'hp8'
charset_nr[73] = 'keybcs2'
charset_nr[74] = 'koi8r'
charset_nr[75] = 'koi8u'
charset_nr[77] = 'latin2'
charset_nr[78] = 'latin5'
charset_nr[79] = 'latin7'
charset_nr[80] = 'cp850'
charset_nr[81] = 'cp852'
charset_nr[82] = 'swe7'
charset_nr[83] = 'utf8'
charset_nr[84] = 'big5'
charset_nr[85] = 'euckr'
charset_nr[86] = 'gb2312'
charset_nr[87] = 'gbk'
charset_nr[88] = 'sjis'
charset_nr[89] = 'tis620'
charset_nr[90] = 'ucs2'
charset_nr[91] = 'ujis'
charset_nr[92] = 'geostd8'
charset_nr[93] = 'geostd8'
charset_nr[94] = 'latin1'
charset_nr[95] = 'cp932'
charset_nr[96] = 'cp932'
charset_nr[97] = 'eucjpms'
charset_nr[98] = 'eucjpms'
charset_nr[99] = 'cp1250'
for i in range(128, 192):
    charset_nr[i] = 'ucs2'
for i in range(192, 211):
    charset_nr[i] = 'utf8'




class BufferError(Exception):
    pass

class BufferOverflowError(BufferError):
    pass

class BufferUnderflowError(BufferError):
    pass

class BufferInvalidArgumentError(BufferError):
    pass


cdef class Buffer:
    """Creates a :class:`Buffer` object. The buffer class forms the basis for IO in the Concurrence Framework.
    The buffer class represents a mutable array of bytes of that can be read from and written to using the
    read_XXX and write_XXX methods. 
    Operations on the buffer are performed relative to the current :attr:`position` attribute of the buffer.
    A buffer also has a current :attr:`limit` property above which no data may be read or written. 
    If an operation tries to read beyond the current :attr:`limit` a BufferUnderflowError is raised. If an operation 
    tries to write beyond the current :attr:`limit` a BufferOverflowError is raised.
    The general idea of the :class:`Buffer` was shamelessly copied from java NIO. 
    """
    
    cdef unsigned char * _buff
    cdef int _position
    cdef Buffer _parent
    cdef int _capacity
    cdef int _limit
    
    def __cinit__(self, int capacity, Buffer parent = None):
        if parent is not None: 
            #this is a copy contructor for a shallow
            #copy, e.g. we reference the same data as our parent, but have our
            #own position and limit (use .duplicate method to get the copy)
            self._parent = parent #this incs the refcnt on parent
            self._buff = parent._buff
            self._position = parent._position
            self._limit = parent._limit
            self._capacity = parent._capacity
        else:
            #normal constructor
            self._parent = None
            self._capacity = capacity
            self._buff = <unsigned char *>(calloc(1, self._capacity))
        
    def __dealloc__(self):
        if self._parent is None:
            free(self._buff)
        else:
            self._parent = None #releases our refcnt on parent             
        
    def __init__(self, int capacity, Buffer parent = None):
        """Create a new empty buffer with the given *capacity*."""
        self.clear()

    
    def duplicate(self):
        """Return a shallow copy of the Buffer, e.g. the copied buffer 
        references the same bytes as the original buffer, but has its own
        independend position and limit."""
        return Buffer(0, self)
        
    def copy(self, Buffer src, int src_start, int dst_start, int length):
        """Copies *length* bytes from buffer *src*, starting at position *src_start*, to this
        buffer at position *dst_start*."""
        if length < 0:
            raise BufferInvalidArgumentError("length must be >= 0")
        if src_start < 0:
            raise BufferInvalidArgumentError("src start must be >= 0")
        if src_start > src._capacity:
            raise BufferInvalidArgumentError("src start must <= src capacity")
        if src_start + length > src._capacity:
            raise BufferInvalidArgumentError("src start + length must <= src capacity")
        if dst_start < 0:
            raise BufferInvalidArgumentError("dst start must be >= 0")
        if dst_start > self._capacity:
            raise BufferInvalidArgumentError("dst start must <= dst capacity")
        if dst_start + length > self._capacity:
            raise BufferInvalidArgumentError("dst start + length must <= dst capacity")
        #now we can safely copy!
        memcpy(self._buff + dst_start, src._buff + src_start, length)        
        
    def clear(self):
        """Prepares the buffer for relative read operations. The buffers :attr:`limit` will set to the buffers :attr:`capacity` and
        its :attr:`position` will be set to 0."""
        self._limit = self._capacity
        self._position = 0

    def flip(self):
        """Prepares the buffer for relative write operations. The buffers :attr:`limit` will set to the buffers :attr:`position` and
        its :attr:`position` will be set to 0."""
        self._limit = self._position
        self._position = 0

    def rewind(self):
        """Sets the buffers :attr:`position` back to 0."""
        self._position = 0

    cdef int _skip(self, int n) except -1:
        if self._position + n <= self.limit:
            self._position = self._position + n
            return n
        else:
            raise BufferUnderflowError()
        
    def skip(self, int n):
        """Updates the buffers position by skipping n bytes. It is not allowed to skip passed the current :attr:`limit`. 
        In that case a :exc:`BufferUnderflowError` will be raised and the :attr:`position` will remain the same"""
        return self._skip(n)
                
    cdef int _remaining(self):
        return self._limit - self._position


    property capacity:
        def __get__(self):
            return self._capacity
            
    property remaining:
        def __get__(self):
            return self._limit - self._position

    property limit:
        def __get__(self):
            return self._limit
        
        def __set__(self, limit):
            if limit >= 0 and limit <= self._capacity and limit >= self._position:
                self._limit = limit
            else:
                if limit < 0:
                    raise BufferInvalidArgumentError("limit must be >= 0")
                elif limit > self._capacity:
                    raise BufferInvalidArgumentError("limit must be <= capacity")
                elif limit < self._position:
                    raise BufferInvalidArgumentError("limit must be >= position")
                else:
                    raise BufferInvalidArgumentError() 

    property position:
        def __get__(self):
            return self._position
        
        def __set__(self, position):
            if position >= 0 and position <= self._capacity and position <= self._limit:
                self._position = position
            else:
                if position < 0:
                    raise BufferInvalidArgumentError("position must be >= 0")
                elif position > self._capacity:
                    raise BufferInvalidArgumentError("position must be <= capacity")
                elif position > self._limit:
                    raise BufferInvalidArgumentError("position must be <= limit")
                else:                    
                    raise BufferInvalidArgumentError()
                                                
    cdef int _read_byte(self) except -1:
        cdef int b
        if self._position + 1 <= self._limit:             
            b = self._buff[self._position]
            self._position = self._position + 1
            return b
        else:
            raise BufferUnderflowError()
                                                        
    def read_byte(self):
        """Reads and returns a single byte from the buffer and updates the :attr:`position` by 1."""
        return self._read_byte()
        
    def recv(self, int fd):
        """Reads as many bytes as will fit up till the :attr:`limit` of the buffer from the filedescriptor *fd*.
        Returns a tuple (bytes_read, bytes_remaining). If *bytes_read* is negative, a IO Error was encountered. 
        The :attr:`position` of the buffer will be updated according to the number of bytes read.
        """
        cdef int b
        b = 0
        #TODO
        #b = read(fd, self._buff + self._position, self._limit - self._position)
        if b > 0: self._position = self._position + b
        return b, self._limit - self._position

    def send(self, int fd):
        """Sends as many bytes as possible up till the :attr:`limit` of the buffer to the filedescriptor *fd*.
        Returns a tuple (bytes_written, bytes_remaining). If *bytes_written* is negative, an IO Error was encountered.
        """
        cdef int b
        b = 0
        #TODO
        #b = write(fd, self._buff + self._position, self._limit - self._position)

        if b > 0: self._position = self._position + b
        return b, self._limit - self._position
        
    def compact(self):
        """Prepares the buffer again for relative reading, but any left over data still present in the buffer (the bytes between
        the current :attr:`position` and current :attr:`limit`) will be copied to the start of the buffer. The position of the buffer
        will be right after the copied data.
        """
        cdef int n
        n = self._limit - self._position 
        if n > 0 and self._position > 0:
            if n < self._position: 
                memcpy(self._buff + 0, self._buff + self._position, n)
            else:
                memmove(self._buff + 0, self._buff + self._position, n)
        self._position = n
        self._limit = self._capacity

    def __getitem__(self, object i):
        cdef int start, end, stride
        if type(i) == types.IntType:
            if i >= 0 and i < self._capacity:
                return self._buff[i]
            else:        
                raise BufferInvalidArgumentError("index must be >= 0 and < capacity")
        elif type(i) == types.SliceType:
            start, end, stride = i.indices(self._capacity)
            return PyString_FromStringAndSize(<char *>(self._buff + start), end - start)
        else:
            raise BufferInvalidArgumentError("wrong index type")

    def __setitem__(self, object i, object value):
        cdef int start, end, stride
        cdef char *b 
        cdef Py_ssize_t n
        if type(i) == types.IntType:
            if type(value) != types.IntType:
                raise BufferInvalidArgumentError("value must be integer")
            if value < 0 or value > 255:
                raise BufferInvalidArgumentError("value must in range [0..255]")
            if i >= 0 and i < self._capacity:
                self._buff[i] = value
            else:
                raise BufferInvalidArgumentError("index must be >= 0 and < capacity")
        elif type(i) == types.SliceType:
            start, end, stride = i.indices(self._capacity)
            PyString_AsStringAndSize(value, &b, &n)
            if n != (end - start):
                raise BufferInvalidArgumentError("incompatible slice")
            memcpy(self._buff + start, b, n)
        else:
            raise BufferInvalidArgumentError("wrong index type")
        
    def read_short(self):
        """Read a 2 byte little endian integer from buffer and updates position."""
        cdef int s
        if 2 > (self._limit - self._position):
            raise BufferUnderflowError()
        else:
             s = self._buff[self._position] + (self._buff[self._position + 1] << 8)
             self._position = self._position + 2
             return s
        
    cdef object _read_bytes(self, int n):
        """reads n bytes from buffer, updates position, and returns bytes as a python string"""
        if n > (self._limit - self._position):
            raise BufferUnderflowError()
        else:
            s = PyString_FromStringAndSize(<char *>(self._buff + self._position), n)
            self._position = self._position + n
            return s
            
    def read_bytes(self, int n = -1):
        """Reads n bytes from buffer, updates position, and returns bytes as a python string,
        if there are no n bytes available, a :exc:`BufferUnderflowError` is raised."""
        if n == -1:
            return self._read_bytes(self._limit - self._position)
        else:
            return self._read_bytes(n)
    
    def read_bytes_until(self, int b):
        """Reads bytes until character b is found, or end of buffer is reached in which case it will raise a :exc:`BufferUnderflowError`."""
        cdef int n, maxlen
        cdef char *zpos, *start 
        if b < 0 or b > 255:
            raise BufferInvalidArgumentError("b must in range [0..255]")
        maxlen = self._limit - self._position
        start = <char *>(self._buff + self._position)
        zpos = <char *>(memchr(start, b, maxlen))
        if zpos == NULL:
            raise BufferUnderflowError()
        else:
            n = zpos - start
            s = PyString_FromStringAndSize(start, n)
            self._position = self._position + n + 1
            return s

    def read_line(self, int include_separator = 0):
        """Reads a single line of bytes from the buffer where the end of the line is indicated by either 'LF' or 'CRLF'.
        The line will be returned as a string not including the line-separator. Optionally *include_separator* can be specified
        to make the method to also return the line-separator."""
        cdef int n, maxlen
        cdef char *zpos, *start 
        maxlen = self._limit - self._position
        start = <char *>(self._buff + self._position)
        zpos = <char *>(memchr(start, 10, maxlen))
        if maxlen == 0:
            raise BufferUnderflowError()
        if zpos == NULL:
            raise BufferUnderflowError()
        n = zpos - start
        if self._buff[self._position + n - 1] == 13: #\r\n
            if include_separator:
                s = PyString_FromStringAndSize(start, n + 1)
                self._position = self._position + n + 1
            else:
                s = PyString_FromStringAndSize(start, n - 1)
                self._position = self._position + n + 1
        else: #\n
            if include_separator:
                s = PyString_FromStringAndSize(start, n + 1)
                self._position = self._position + n + 1
            else:
                s = PyString_FromStringAndSize(start, n)
                self._position = self._position + n + 1                                    
        return s
    
    def write_bytes(self, s):
        """Writes a number of bytes given by the python string s to the buffer and updates position. Raises 
        :exc:`BufferOverflowError` if you try to write beyond the current :attr:`limit`."""
        cdef char *b 
        cdef Py_ssize_t n
        PyString_AsStringAndSize(s, &b, &n)
        if n > (self._limit - self._position):
            raise BufferOverflowError()
        else:
            memcpy(self._buff + self._position, b, n)
            self._position = self._position + n
            return n

    def write_buffer(self, Buffer other):
        """writes available bytes from other buffer to this buffer"""
        self.write_bytes(other.read_bytes(-1)) #TODO use copy
                
    cdef int _write_byte(self, unsigned int b) except -1:
        """writes a single byte to the buffer and updates position"""
        if self._position + 1 <= self._limit:             
            self._buff[self._position] = b
            self._position = self._position + 1
            return 1
        else:
            raise BufferOverflowError()

    def write_byte(self, unsigned int b):
        """writes a single byte to the buffer and updates position"""
        return self._write_byte(b)

    def write_int(self, unsigned int i):
        """writes a 32 bit integer to the buffer and updates position (little-endian)"""
        if self._position + 4 <= self._limit:             
            self._buff[self._position + 0] = (i >> 0) & 0xFF
            self._buff[self._position + 1] = (i >> 8) & 0xFF
            self._buff[self._position + 2] = (i >> 16) & 0xFF
            self._buff[self._position + 3] = (i >> 24) & 0xFF
            self._position = self._position + 4
            return 4
        else:
            raise BufferOverflowError()

    def write_short(self, unsigned int i):
        """writes a 16 bit integer to the buffer and updates position (little-endian)"""
        if self._position + 2 <= self._limit:             
            self._buff[self._position + 0] = (i >> 0) & 0xFF
            self._buff[self._position + 1] = (i >> 8) & 0xFF
            self._position = self._position + 2
            return 2
        else:
            raise BufferOverflowError()

    def hex_dump(self, out = None):
        highlight1 = "\033[34m"
        highlight2 = "\033[32m"
        default = "\033[0m"

        if out is None: out = sys.stdout

        import string

        out.write('<concurrence.io.Buffer id=%x, position=%d, limit=%d, capacity=%d>\n' % (id(self), self.position, self.limit, self._capacity))
        printable = set(string.printable)
        whitespace = set(string.whitespace)
        x = 0
        s1 = []
        s2 = []
        while x < self._capacity:
            v = self[x]
            if x < self.position:
                s1.append('%s%02x%s' % (highlight1, v, default))
            elif x < self.limit:
                s1.append('%s%02x%s' % (highlight2, v, default))
            else:
                s1.append('%02x' % v)
            c = chr(v)
            if c in printable and not c in whitespace:
                s2.append(c)
            else:
                s2.append('.')
            x += 1
            if x % 16 == 0:
                out.write('%04x' % (x - 16) + '  ' + ' '.join(s1[:8]) + '  ' + ' '.join(s1[8:]) + '  ' + ''.join(s2[:8]) + ' ' + (''.join(s2[8:]) + '\n'))
                s1 = []
                s2 = []
        out.flush()
        
    def __repr__(self):
        import cStringIO
        sio = cStringIO.StringIO()
        self.hex_dump(sio)
        return sio.getvalue()
    
    def __str__(self):
        return repr(self)


class PacketReadError(Exception):
    pass

MAX_PACKET_SIZE = 4 * 1024 * 1024 #4mb
            
cdef class PacketReader:

    cdef int oversize
    cdef readonly int number
    cdef readonly int length #length in bytes of the current packet in the buffer
    cdef readonly int command
    cdef readonly int start #position of start of packet in buffer
    cdef readonly int end
    
    cdef public object encoding
    cdef public object use_unicode
    
    cdef readonly Buffer buffer #the current read buffer
    cdef readonly Buffer packet #the current packet (could be normal or oversize packet):
    
    cdef Buffer normal_packet #the normal packet
    cdef Buffer oversize_packet #if we are reading an oversize packet, this is where we keep the data    
    
    def __init__(self, Buffer buffer):
        self.oversize = 0
        self.encoding = None
        self.use_unicode = False
        self.buffer = buffer

        self.normal_packet = buffer.duplicate()
        self.oversize_packet = buffer.duplicate()
        self.packet = self.normal_packet         

    cdef int _read(self) except PACKET_READ_ERROR:
        """this method scans the buffer for packets, reporting the start, end of packet
        or whether the packet in the buffer is incomplete and more data is needed"""
        
        cdef int r
        cdef Buffer buffer
        
        buffer = self.buffer
                
        self.command = 0
        self.start = 0
        self.end = 0
                        
        r = buffer._remaining()
        
        if self.oversize == 0: #normal packet reading mode
            #print 'normal mode', r

            if r < 4:
                #print 'rem < 4 return' 
                return PACKET_READ_NONE #incomplete header
            
            #these four reads will always succeed because r >= 4
            self.length = (buffer._read_byte()) + (buffer._read_byte() << 8) + (buffer._read_byte() << 16) + 4
            self.number = buffer._read_byte()
            
            if self.length <= r:
                #a complete packet sitting in buffer                
                self.start = buffer._position - 4
                self.end = self.start + self.length
                self.command = buffer._buff[buffer._position]
                buffer._skip(self.length - 4) #skip rest of packet
                #print 'single packet recvd', self.length, self.command
                if self.length < r: 
                    return PACKET_READ_TRUE | PACKET_READ_START | PACKET_READ_END | PACKET_READ_MORE
                else:
                    return PACKET_READ_TRUE | PACKET_READ_START | PACKET_READ_END
                #return self.length < r #if l was smaller, tere is more, otherwise l == r and buffer is empty                   
            else:
                #print 'incomplete packet in buffer', buffer._position, self.length 
                if self.length > buffer._capacity:
                    #print 'start of oversize packet', self.length
                    self.start = buffer._position - 4
                    self.end = buffer._limit
                    self.command = buffer._buff[buffer._position]
                    buffer._position = buffer._limit #skip rest of buffer
                    self.oversize = self.length - r#left todo
                    return PACKET_READ_TRUE | PACKET_READ_START
                else:
                    #print 'small incomplete packet', self.length, buffer._position
                    buffer._skip(-4) #rewind to start of incomplete packet
                    return PACKET_READ_NONE #incomplete packet
                
        else: #busy reading an oversized packet
            #print 'oversize mode', r, self.oversize, buffer.position, buffer.limit
            self.start = buffer._position

            if self.oversize < r:
                buffer._skip(self.oversize) #skip rest of buffer
                self.oversize = 0
            else:
                buffer._skip(r) #skip rest of buffer or remaining oversize
                self.oversize = self.oversize - r
            
            self.end = buffer._position
             
            if self.oversize == 0:
                #print 'oversize packet recvd'
                return PACKET_READ_TRUE | PACKET_READ_END | PACKET_READ_MORE
            else:
                #print 'some data of oversize packet recvd'
                return PACKET_READ_TRUE
                
    def read(self):
        return self._read()
        
    cdef int _read_packet(self) except PACKET_READ_ERROR:
        cdef int r, size, max_packet_size
        r = self._read()
        if r & PACKET_READ_TRUE:
            if (r & PACKET_READ_START) and (r & PACKET_READ_END):
                #normal sized packet, read entirely
                self.packet = self.normal_packet
                self.packet._position, self.packet._limit = self.start + 4, self.end
            elif (r & PACKET_READ_START) and not (r & PACKET_READ_END):
                #print 'start of oversize', self.end - self.start, self.length
                #first create oversize_packet if necessary:
                if self.oversize_packet._capacity < self.length:
                    #find first size multiple of 2 that will fit the oversize packet
                    size = self.buffer._capacity
                    while size < self.length:
                        size = size * 2
                    if size >= MAX_PACKET_SIZE:
                        raise PacketReadError("oversized packet will not fit in MAX_PACKET_SIZE, length: %d, MAX_PACKET_SIZE: %d" % (self.length, MAX_PACKET_SIZE))
                    #print 'createing oversize packet', size
                    self.oversize_packet = Buffer(size)
                self.oversize_packet.copy(self.buffer, self.start, 0, self.end - self.start)
                self.packet = self.oversize_packet
                self.packet._position, self.packet._limit = 4, self.end - self.start
            else:
                #end or middle part of oversized packet
                self.oversize_packet.copy(self.buffer, self.start, self.oversize_packet._limit, self.end - self.start)
                self.oversize_packet._limit = self.oversize_packet._limit + (self.end - self.start) 
                
        return r

    def read_packet(self):
        return self._read_packet()

    cdef _read_length_coded_binary(self):
        cdef unsigned int n, v
        cdef unsigned long long vw
        cdef Buffer packet

        packet = self.packet
        if packet._position + 1 > packet._limit: raise  BufferUnderflowError()        
        n = packet._buff[packet._position]
        if n < 251:
            packet._position = packet._position + 1
            return n
        elif n == 251:
            assert False, 'unexpected, only valid for row data packet'
        elif n == 252:
            #16 bit word
            if packet._position + 3 > packet._limit: raise  BufferUnderflowError()
            v = packet._buff[packet._position + 1] | ((packet._buff[packet._position + 2]) << 8)
            packet._position = packet._position + 3
            return v              
        elif n == 253:
            #24 bit word
            if packet._position + 4 > packet._limit: raise  BufferUnderflowError()
            v = packet._buff[packet._position + 1] | ((packet._buff[packet._position + 2]) << 8) | ((packet._buff[packet._position + 3]) << 16)
            packet._position = packet._position + 4
            return v
        else:
            #64 bit word
            if packet._position + 9 > packet._limit: raise  BufferUnderflowError()
            vw = 0
            vw |= (<unsigned long long>packet._buff[packet._position + 1]) << 0
            vw |= (<unsigned long long>packet._buff[packet._position + 2]) << 8
            vw |= (<unsigned long long>packet._buff[packet._position + 3]) << 16
            vw |= (<unsigned long long>packet._buff[packet._position + 4]) << 24 
            vw |= (<unsigned long long>packet._buff[packet._position + 5]) << 32
            vw |= (<unsigned long long>packet._buff[packet._position + 6]) << 40
            vw |= (<unsigned long long>packet._buff[packet._position + 7]) << 48
            vw |= (<unsigned long long>packet._buff[packet._position + 8]) << 56
            packet._position = packet._position + 9
            return vw

    def read_length_coded_binary(self):
        return self._read_length_coded_binary()
            
    cdef _read_bytes_length_coded(self):
        cdef unsigned int n, w
        cdef Buffer packet
        
        packet = self.packet
        if packet._position + 1 > packet._limit: raise  BufferUnderflowError()        
        n = packet._buff[packet._position]
        w = 1
        if n >= 251:
            if n == 251:
                packet._position = packet._position + 1
                return None
            elif n == 252:
                if packet._position + 2 > packet._limit: raise  BufferUnderflowError()
                n = packet._buff[packet._position + 1] | ((packet._buff[packet._position + 2]) << 8)  
                w = 3
            elif n == 253:
                #24 bit word
                if packet._position + 4 > packet._limit: raise  BufferUnderflowError()
                n = packet._buff[packet._position + 1] | ((packet._buff[packet._position + 2]) << 8) | ((packet._buff[packet._position + 3]) << 16)
                w = 4
            elif n == 254:
                #64 bit word
                if packet._position + 9 > packet._limit: raise  BufferUnderflowError()
                n = 0
                n |= (<unsigned long long>packet._buff[packet._position + 1]) << 0
                n |= (<unsigned long long>packet._buff[packet._position + 2]) << 8
                n |= (<unsigned long long>packet._buff[packet._position + 3]) << 16
                n |= (<unsigned long long>packet._buff[packet._position + 4]) << 24
                n |= (<unsigned long long>packet._buff[packet._position + 5]) << 32
                n |= (<unsigned long long>packet._buff[packet._position + 6]) << 40
                n |= (<unsigned long long>packet._buff[packet._position + 7]) << 48
                n |= (<unsigned long long>packet._buff[packet._position + 8]) << 56
                w = 9
           
            else:
                assert False, 'not implemented yet, n: %02x' % n
        
        if (n + w) > (packet._limit - packet._position):
            raise BufferUnderflowError()
        packet._position = packet._position + w
        s = PyString_FromStringAndSize(<char *>(packet._buff + packet._position), n)
        packet._position = packet._position + n
        return s
        
    def read_bytes_length_coded(self):
        return self._read_bytes_length_coded()
    
    def read_field_type(self):
        cdef int n
        cdef Buffer packet
        
        packet = self.packet
        n = packet._read_byte()
        packet._skip(n) #catalog
        n = packet._read_byte()
        packet._skip(n) #db
        n = packet._read_byte()
        packet._skip(n) #table
        n = packet._read_byte()
        packet._skip(n) #org_table
        n = packet._read_byte()
        name = packet._read_bytes(n)
        n = packet._read_byte()
        packet._skip(n) #org_name
        packet._skip(1)
        charsetnr = packet._read_bytes(2)
        n = packet._skip(4)
        n = packet.read_byte() #type
        return (name, n, charsetnr)
        
    cdef _string_to_int(self, object s):
        if s == None:
            return None
        else:
            return int(s)

    cdef _string_to_float(self, object s):
        if s == None:
            return None
        else:
            return float(s)

    cdef _read_datestring(self):
        cdef unsigned int n
        cdef Buffer packet

        packet = self.packet
        if packet._position + 1 > packet._limit: raise BufferUnderflowError()
        n = packet._buff[packet._position]

        if n == 251:
            packet._position = packet._position + 1
            return None
        
        packet._position = packet._position + 1
        s = PyString_FromStringAndSize(<char *>(packet._buff + packet._position), n)
        packet._position = packet._position + n
        return s


    cdef _datestring_to_date(self, object s):
        if not s or s == "0000-00-00":
            return None

        parts = s.split("-")
        try:
            assert len(parts) == 3
            d = datetime.date(*map(int, parts))
        except (AssertionError, ValueError):
            raise ValueError("Unhandled date format: %r" % (s, ))
        
        return d

    cdef _datestring_to_datetime(self, object s):
        if not s:
            return None
            
        datestring, timestring = s.split(" ")

        _date = self._datestring_to_date(datestring)
        if _date is None:
            return None

        parts = timestring.split(":")
        try:
            assert len(parts) == 3
            d = datetime.datetime(_date.year, _date.month, _date.day, *map(int, parts))
        except (AssertionError, ValueError):
            raise ValueError("Unhandled datetime format: %r" % (s, ))

        return d
    cdef int _read_row(self, object row, object fields, int field_count) except PACKET_READ_ERROR:
        cdef int i, r
        cdef int decode
        
        if self.encoding: 
            decode = 1
            encoding = self.encoding
        else:
            decode = 0

        r = self._read_packet()
        if r & PACKET_READ_END: #whole packet recv                    
            if self.packet._buff[self.packet._position] == 0xFE: 
                return r | PACKET_READ_EOF
            else:
                i = 0
                int_types = INT_TYPES
                float_types = FLOAT_TYPES
                string_types = STRING_TYPES
                date_type = FIELD_TYPE.DATE
                datetime_type = FIELD_TYPE.DATETIME
                while i < field_count:
                    t = fields[i][1] #type_code
                    if t in int_types:
                        row[i] = self._string_to_int(self._read_bytes_length_coded())
                    elif t in string_types:
                        row[i] = self._read_bytes_length_coded()
                        if row[i] is not None and (self.encoding or self.use_unicode):
                            bytes = fields[i][2]
                            nr = ord(bytes[1]) << 8 | ord(bytes[0])
                            row[i] = row[i].decode(charset_nr[nr])
                            if not self.use_unicode:
                                row[i] = row[i].encode(self.encoding)

                    elif t in float_types:
                        row[i] = self._string_to_float(self._read_bytes_length_coded())
                    elif t  == date_type:
                        row[i] = self._datestring_to_date(self._read_datestring())
                    elif t  == datetime_type:
                        row[i] = self._datestring_to_datetime(self._read_datestring())
                    else:
                        row[i] = self._read_bytes_length_coded()

                    i = i + 1
        return r
    
    def read_rows(self, object fields, int row_count):
        cdef int r, i, field_count
        field_count = len(fields)
        i = 0
        r = 0
        rows = []
        row = [None] * field_count
        add = rows.append
        #print "Reading fields", len(fields)
        while i < row_count:
            r = self._read_row(row, fields, field_count)
            if r & PACKET_READ_END:
                if r & PACKET_READ_EOF:
                    break
                else:
                    add(tuple(row))
            if not (r & PACKET_READ_MORE):
                break
            i = i + 1
        return r, rows
    
cdef enum:
    PROXY_STATE_UNDEFINED = -2
    PROXY_STATE_ERROR = -1
    PROXY_STATE_INIT = 0
    PROXY_STATE_READ_AUTH = 1
    PROXY_STATE_READ_AUTH_RESULT = 2
    PROXY_STATE_READ_AUTH_OLD_PASSWORD = 3
    PROXY_STATE_READ_AUTH_OLD_PASSWORD_RESULT = 4
    PROXY_STATE_READ_COMMAND = 5
    PROXY_STATE_READ_RESULT = 6
    PROXY_STATE_READ_RESULT_FIELDS = 7
    PROXY_STATE_READ_RESULT_ROWS = 8
    PROXY_STATE_READ_RESULT_FIELDS_ONLY = 9
    PROXY_STATE_FINISHED = 10
    
class PROXY_STATE:
    UNDEFINED = PROXY_STATE_UNDEFINED
    ERROR = PROXY_STATE_ERROR
    INIT = PROXY_STATE_INIT
    FINISHED = PROXY_STATE_FINISHED
    READ_AUTH = PROXY_STATE_READ_AUTH
    READ_AUTH_RESULT = PROXY_STATE_READ_AUTH_RESULT
    READ_AUTH_OLD_PASSWORD = PROXY_STATE_READ_AUTH_OLD_PASSWORD
    READ_AUTH_OLD_PASSWORD_RESULT = PROXY_STATE_READ_AUTH_OLD_PASSWORD_RESULT
    READ_COMMAND = PROXY_STATE_READ_COMMAND
    READ_RESULT = PROXY_STATE_READ_RESULT
    READ_RESULT_FIELDS = PROXY_STATE_READ_RESULT_FIELDS
    READ_RESULT_ROWS = PROXY_STATE_READ_RESULT_ROWS
    READ_RESULT_FIELDS_ONLY = PROXY_STATE_READ_RESULT_FIELDS_ONLY
    
SERVER_STATES = set([PROXY_STATE.INIT, PROXY_STATE.READ_AUTH_RESULT, PROXY_STATE.READ_AUTH_OLD_PASSWORD_RESULT,
                     PROXY_STATE.READ_RESULT, PROXY_STATE.READ_RESULT_FIELDS, PROXY_STATE.READ_RESULT_ROWS,
                     PROXY_STATE.READ_RESULT_FIELDS_ONLY, PROXY_STATE.FINISHED])

CLIENT_STATES = set([PROXY_STATE.READ_AUTH, PROXY_STATE.READ_AUTH_OLD_PASSWORD, PROXY_STATE.READ_COMMAND])

AUTH_RESULT_STATES = set([PROXY_STATE.READ_AUTH_OLD_PASSWORD_RESULT, PROXY_STATE.READ_AUTH_RESULT])

READ_RESULT_STATES = set([PROXY_STATE.READ_RESULT, PROXY_STATE.READ_RESULT_FIELDS, PROXY_STATE.READ_RESULT_ROWS, PROXY_STATE.READ_RESULT_FIELDS_ONLY])

class ProxyProtocolException(Exception):
    pass
    
cdef class ProxyProtocol:
    cdef readonly int state
    cdef readonly int number
    
    def __init__(self, initial_state = PROXY_STATE_INIT):
        self.reset(initial_state)
        
    def reset(self, int state):
        self.state = state 
        self.number = 0
        
    cdef int _check_number(self, PacketReader reader) except -1:
        if self.state == PROXY_STATE_READ_COMMAND: 
            self.number = 0
        if self.number != reader.number:
            self.state = PROXY_STATE_ERROR 
            raise ProxyProtocolException('packet number out of sync')
        self.number = self.number + 1
        self.number = self.number % 256
        
    def read_server(self, PacketReader reader):
        cdef int read_result, prev_state
        
        prev_state = self.state
        
        while 1:
            
            read_result = reader._read()
            
            if read_result & PACKET_READ_START: 
                self._check_number(reader)
        
            if read_result & PACKET_READ_END: #packet recvd
                if self.state == PROXY_STATE_INIT:
                    #server handshake recvd
                    #server could have send error instead of inital handshake
                    self.state = PROXY_STATE_READ_AUTH
                elif self.state == PROXY_STATE_READ_AUTH_RESULT:
                    #server auth result recvd
                    if reader.command == 0xFE:
                        self.state = PROXY_STATE_READ_AUTH_OLD_PASSWORD
                    elif reader.command == 0x00: #OK
                        self.state = PROXY_STATE_READ_COMMAND                
                elif self.state == PROXY_STATE_READ_AUTH_OLD_PASSWORD_RESULT:
                    #server auth old password result recvd 
                    self.state = PROXY_STATE_READ_COMMAND
                elif self.state == PROXY_STATE_READ_RESULT:            
                    if reader.command == 0x00: #no result set but ok
                        #server result recvd OK
                        self.state = PROXY_STATE_READ_COMMAND
                    elif reader.command == 0xFF: 
                        #no result set error
                        self.state = PROXY_STATE_READ_COMMAND
                    else:
                        #server result recv result set header
                        self.state = PROXY_STATE_READ_RESULT_FIELDS
                elif self.state == PROXY_STATE_READ_RESULT_FIELDS:
                    if reader.command == 0xFE: #EOF for fields
                        #server result fields recvd
                        self.state = PROXY_STATE_READ_RESULT_ROWS
                elif self.state == PROXY_STATE_READ_RESULT_ROWS:
                    if reader.command == 0xFE: #EOF for rows
                        #server result rows recvd
                        self.state = PROXY_STATE_READ_COMMAND
                elif self.state == PROXY_STATE_READ_RESULT_FIELDS_ONLY:
                    if reader.command == 0xFE: #EOF for fields
                        #server result fields only recvd
                        self.state = PROXY_STATE_READ_COMMAND
                else:
                    self.state = PROXY_STATE_ERROR
                    raise ProxyProtocolException('unexpected packet')

            if self.state != prev_state:
                break
                    
            if not (read_result & PACKET_READ_MORE):
                break           
               
        return read_result, self.state, prev_state
                                            
    def read_client(self, PacketReader reader):
        cdef int read_result, prev_state
        
        prev_state = self.state
        
        while 1:
            
            read_result = reader._read()
            
            if read_result & PACKET_READ_START: 
                self._check_number(reader)
        
            if read_result & PACKET_READ_END: #packet recvd
                if self.state == PROXY_STATE_READ_AUTH:
                    #client auth recvd
                    self.state = PROXY_STATE_READ_AUTH_RESULT
                elif self.state == PROXY_STATE_READ_AUTH_OLD_PASSWORD:
                    #client auth old pwd recvd    
                    self.state = PROXY_STATE_READ_AUTH_OLD_PASSWORD_RESULT
                elif self.state == PROXY_STATE_READ_COMMAND:
                    #client cmd recvd
                    if reader.command == COMMAND_LIST: #list cmd
                        self.state = PROXY_STATE_READ_RESULT_FIELDS_ONLY
                    elif reader.command == COMMAND_QUIT: #COM_QUIT
                        self.state = PROXY_STATE_FINISHED
                    else:                
                        self.state = PROXY_STATE_READ_RESULT
                else:
                    self.state = PROXY_STATE_ERROR
                    raise ProxyProtocolException('unexpected packet')

            if self.state != prev_state:
                break
            
            if not (read_result & PACKET_READ_MORE):
                break           
                                     

        return read_result, self.state, prev_state    
    
