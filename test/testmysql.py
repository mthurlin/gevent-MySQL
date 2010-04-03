# -*- coding: latin1 -*-
from __future__ import with_statement

import time
import datetime
import logging
import unittest
import gevent

import geventmysql as dbapi
from geventmysql import client
from geventmysql._mysql import PacketReadError

DB_HOST = '127.0.0.1:3306'
DB_USER = 'gevent_test'
DB_PASSWD = 'gevent_test'
DB_DB = 'gevent_test'

class TestMySQL(unittest.TestCase):
    log = logging.getLogger('TestMySQL')

    def testMySQLClient(self):
        cnn = client.connect(host = DB_HOST, user = DB_USER,
                             password = DB_PASSWD, db = DB_DB)

        rs = cnn.query("select 1")

        self.assertEqual([(1,)], list(rs))

        rs.close()
        cnn.close()

    def testConnectNoDb(self):
        cnn = client.connect(host = DB_HOST, user = DB_USER, password = DB_PASSWD)

        rs = cnn.query("select 1")

        self.assertEqual([(1,)], list(rs))

        rs.close()
        cnn.close()


    def testMySQLClient2(self):
        cnn = client.connect(host = DB_HOST, user = DB_USER,
                             password = DB_PASSWD, db = DB_DB)

        cnn.query("truncate tbltest")

        for i in range(10):
            self.assertEquals((1, 0), cnn.query("insert into tbltest (test_id, test_string) values (%d, 'test%d')" % (i, i)))

        rs = cnn.query("select test_id, test_string from tbltest")

        #trying to close it now would give an error, e.g. we always need to read
        #the result from the database otherwise connection would be in wrong stat
        try:
            rs.close()
            self.fail('expected exception')
        except client.ClientProgrammingError:
            pass

        for i, row in enumerate(rs):
            self.assertEquals((i, 'test%d' % i), row)

        rs.close()
        cnn.close()

    def testMySQLTimeout(self):
        cnn = client.connect(host = DB_HOST, user = DB_USER,
                             password = DB_PASSWD, db = DB_DB)

        rs = cnn.query("select sleep(2)")
        list(rs)
        rs.close()

        from gevent import Timeout

        start = time.time()
        try:
            def delay():
                cnn.query("select sleep(4)")
                self.fail('expected timeout')
            gevent.with_timeout(2, delay)
        except Timeout:
            end = time.time()
            self.assertAlmostEqual(2.0, end - start, places = 1)

        cnn.close()

    def testParallelQuery(self):

        def query(s):
            cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                                password = DB_PASSWD, db = DB_DB)
            cur = cnn.cursor()
            cur.execute("select sleep(%d)" % s)
            cur.close()
            cnn.close()

        start = time.time()
        ch1 = gevent.spawn(query, 1)
        ch2 = gevent.spawn(query, 2)
        ch3 = gevent.spawn(query, 3)
        gevent.joinall([ch1, ch2, ch3])

        end = time.time()
        self.assertAlmostEqual(3.0, end - start, places = 1)

    def testMySQLDBAPI(self):

        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB)

        cur = cnn.cursor()

        cur.execute("truncate tbltest")

        for i in range(10):
            cur.execute("insert into tbltest (test_id, test_string) values (%d, 'test%d')" % (i, i))

        cur.close()

        cur = cnn.cursor()

        cur.execute("select test_id, test_string from tbltest")

        self.assertEquals((0, 'test0'), cur.fetchone())

        #check that fetchall gets the remainder
        self.assertEquals([(1, 'test1'), (2, 'test2'), (3, 'test3'), (4, 'test4'), (5, 'test5'), (6, 'test6'), (7, 'test7'), (8, 'test8'), (9, 'test9')], cur.fetchall())

        #another query on the same cursor should work
        cur.execute("select test_id, test_string from tbltest")

        #fetch some but not all
        self.assertEquals((0, 'test0'), cur.fetchone())
        self.assertEquals((1, 'test1'), cur.fetchone())
        self.assertEquals((2, 'test2'), cur.fetchone())

        #close whould work even with half read resultset
        cur.close()

        #this should not work, cursor was closed
        try:
            cur.execute("select * from tbltest")
            self.fail("expected exception")
        except dbapi.ProgrammingError:
            pass

    def testLargePackets(self):
        cnn = client.connect(host = DB_HOST, user = DB_USER,
                             password = DB_PASSWD, db = DB_DB)


        cnn.query("truncate tbltest")

        c = cnn.buffer.capacity

        blob = '0123456789'
        while 1:
            cnn.query("insert into tbltest (test_id, test_blob) values (%d, '%s')" % (len(blob), blob))
            if len(blob) > (c * 2): break
            blob = blob * 2

        rs = cnn.query("select test_id, test_blob from tbltest")
        for row in rs:
            self.assertEquals(row[0], len(row[1]))
            self.assertEquals(blob[:row[0]], row[1])
        rs.close()

        #reread, second time, oversize packet is already present
        rs = cnn.query("select test_id, test_blob from tbltest")
        for row in rs:
            self.assertEquals(row[0], len(row[1]))
            self.assertEquals(blob[:row[0]], row[1])
        rs.close()
        cnn.close()

        #have a very low max packet size for oversize packets
        #and check that exception is thrown when trying to read larger packets
        from geventmysql import _mysql
        _mysql.MAX_PACKET_SIZE = 1024 * 4

        cnn = client.connect(host = DB_HOST, user = DB_USER,
                             password = DB_PASSWD, db = DB_DB)

        try:
            rs = cnn.query("select test_id, test_blob from tbltest")
            for row in rs:
                self.assertEquals(row[0], len(row[1]))
                self.assertEquals(blob[:row[0]], row[1])
            self.fail()
        except PacketReadError:
            pass
        finally:
            try:
                rs.close()
            except:
                pass
            cnn.close()

    def testEscapeArgs(self):
        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB)

        cur = cnn.cursor()

        cur.execute("truncate tbltest")

        cur.execute("insert into tbltest (test_id, test_string) values (%s, %s)", (1, 'piet'))
        cur.execute("insert into tbltest (test_id, test_string) values (%s, %s)", (2, 'klaas'))
        cur.execute("insert into tbltest (test_id, test_string) values (%s, %s)", (3, "pi'et"))

        #classic sql injection, would return all rows if no proper escaping is done
        cur.execute("select test_id, test_string from tbltest where test_string = %s", ("piet' OR 'a' = 'a",))
        self.assertEquals([], cur.fetchall()) #assert no rows are found

        #but we should still be able to find the piet with the apostrophe in its name
        cur.execute("select test_id, test_string from tbltest where test_string = %s", ("pi'et",))
        self.assertEquals([(3, "pi'et")], cur.fetchall())

        #also we should be able to insert and retrieve blob/string with all possible bytes transparently
        chars = ''.join([chr(i) for i in range(256)])


        cur.execute("insert into tbltest (test_id, test_string, test_blob) values (%s, %s, %s)", (4, chars, chars))

        cur.execute("select test_string, test_blob from tbltest where test_id = %s", (4,))
        #self.assertEquals([(chars, chars)], cur.fetchall())
        s, b  = cur.fetchall()[0]

        #test blob
        self.assertEquals(256, len(b))
        self.assertEquals(chars, b)

        #test string
        self.assertEquals(256, len(s))
        self.assertEquals(chars, s)

        cur.close()

        cnn.close()


    def testSelectUnicode(self):
        s = u'r\xc3\xa4ksm\xc3\xb6rg\xc3\xa5s'



        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'latin-1', use_unicode = True)

        cur = cnn.cursor()

        cur.execute("truncate tbltest")
        cur.execute("insert into tbltest (test_id, test_string) values (%s, %s)", (1, 'piet'))
        cur.execute("insert into tbltest (test_id, test_string) values (%s, %s)", (2, s))
        cur.execute(u"insert into tbltest (test_id, test_string) values (%s, %s)", (3, s))

        cur.execute("select test_id, test_string from tbltest")

        result = cur.fetchall()

        self.assertEquals([(1, u'piet'), (2, s), (3, s)], result)

        #test that we can still cleanly roundtrip a blob, (it should not be encoded if we pass
        #it as 'str' argument), eventhough we pass the qry itself as unicode
        blob = ''.join([chr(i) for i in range(256)])

        cur.execute(u"insert into tbltest (test_id, test_blob) values (%s, %s)", (4, blob))
        cur.execute("select test_blob from tbltest where test_id = %s", (4,))
        b2 = cur.fetchall()[0][0]
        self.assertEquals(str, type(b2))
        self.assertEquals(256, len(b2))
        self.assertEquals(blob, b2)

    def testAutoInc(self):

        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB)

        cur = cnn.cursor()

        cur.execute("truncate tblautoincint")

        cur.execute("ALTER TABLE tblautoincint AUTO_INCREMENT = 100")
        cur.execute("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(100, cur.lastrowid)
        cur.execute("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(101, cur.lastrowid)

        cur.execute("ALTER TABLE tblautoincint AUTO_INCREMENT = 4294967294")
        cur.execute("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(4294967294, cur.lastrowid)
        cur.execute("insert into tblautoincint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(4294967295, cur.lastrowid)

        cur.execute("truncate tblautoincbigint")

        cur.execute("ALTER TABLE tblautoincbigint AUTO_INCREMENT = 100")
        cur.execute("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(100, cur.lastrowid)
        cur.execute("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(101, cur.lastrowid)

        cur.execute("ALTER TABLE tblautoincbigint AUTO_INCREMENT = 18446744073709551614")
        cur.execute("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        self.assertEqual(1, cur.rowcount)
        self.assertEqual(18446744073709551614, cur.lastrowid)
        #this fails on mysql, but that is a mysql problem
        #cur.execute("insert into tblautoincbigint (test_string) values (%s)", ('piet',))
        #self.assertEqual(1, cur.rowcount)
        #self.assertEqual(18446744073709551615, cur.lastrowid)

        cur.close()
        cnn.close()

    def testLengthCodedBinary(self):

        from geventmysql._mysql import Buffer, BufferUnderflowError
        from geventmysql.mysql import PacketReader

        def create_reader(bytes):
            b = Buffer(1024)
            for byte in bytes:
                b.write_byte(byte)
            b.flip()

            p = PacketReader(b)
            p.packet.position = b.position
            p.packet.limit = b.limit
            return p

        p = create_reader([100])
        self.assertEquals(100, p.read_length_coded_binary())
        self.assertEquals(p.packet.position, p.packet.limit)
        try:
            p.read_length_coded_binary()
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        try:
            p = create_reader([252])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        try:
            p = create_reader([252, 0xff])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        p = create_reader([252, 0xff, 0xff])
        self.assertEquals(0xFFFF, p.read_length_coded_binary())
        self.assertEquals(3, p.packet.limit)
        self.assertEquals(3, p.packet.position)


        try:
            p = create_reader([253])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        try:
            p = create_reader([253, 0xff])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        try:
            p = create_reader([253, 0xff, 0xff])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        p = create_reader([253, 0xff, 0xff, 0xff])
        self.assertEquals(0xFFFFFF, p.read_length_coded_binary())
        self.assertEquals(4, p.packet.limit)
        self.assertEquals(4, p.packet.position)

        try:
            p = create_reader([254])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        try:
            p = create_reader([254, 0xff])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        try:
            p = create_reader([254, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])
            p.read_length_coded_binary()
            self.fail('expected underflow')
        except BufferUnderflowError:
            pass
        except:
            self.fail('expected underflow')

        p = create_reader([254, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])

        self.assertEquals(9, p.packet.limit)
        self.assertEquals(0, p.packet.position)
        self.assertEquals(0xFFFFFFFFFFFFFFFFL, p.read_length_coded_binary())
        self.assertEquals(9, p.packet.limit)
        self.assertEquals(9, p.packet.position)


    def testBigInt(self):
        """Tests the behaviour of insert/select with bigint/long."""

        BIGNUM = 112233445566778899

        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'latin-1', use_unicode = True)

        cur = cnn.cursor()

        cur.execute("drop table if exists tblbigint")
        cur.execute("""create table tblbigint (
                            test_id int(11) DEFAULT NULL,
                            test_bigint bigint DEFAULT NULL,
                            test_bigint2 bigint DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1""")
        cur.execute("insert into tblbigint (test_id, test_bigint, test_bigint2) values (%s, " + str(BIGNUM) + ", %s)", (1, BIGNUM))
        cur.execute(u"insert into tblbigint (test_id, test_bigint, test_bigint2) values (%s, " + str(BIGNUM) + ", %s)", (2, BIGNUM))


        # Make sure both our inserts where correct (ie, the big number was not truncated/modified on insert)
        cur.execute("select test_id from tblbigint where test_bigint = test_bigint2")
        result = cur.fetchall()
        self.assertEquals([(1, ), (2, )], result)


        # Make sure select gets the right values (ie, the big number was not truncated/modified when retrieved)
        cur.execute("select test_id, test_bigint, test_bigint2 from tblbigint where test_bigint = test_bigint2")
        result = cur.fetchall()
        self.assertEquals([(1, BIGNUM, BIGNUM), (2, BIGNUM, BIGNUM)], result)


    def testDate(self):
        """Tests the behaviour of insert/select with mysql/DATE <-> python/datetime.date"""

        d_date = datetime.date(2010, 02, 11)
        d_string = "2010-02-11"

        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'latin-1', use_unicode = True)

        cur = cnn.cursor()

        cur.execute("drop table if exists tbldate")
        cur.execute("create table tbldate (test_id int(11) DEFAULT NULL, test_date date DEFAULT NULL, test_date2 date DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cur.execute("insert into tbldate (test_id, test_date, test_date2) values (%s, '" + d_string + "', %s)", (1, d_date))

        # Make sure our insert was correct
        cur.execute("select test_id from tbldate where test_date = test_date2")
        result = cur.fetchall()
        self.assertEquals([(1, )], result)

        # Make sure select gets the right value back
        cur.execute("select test_id, test_date, test_date2 from tbldate where test_date = test_date2")
        result = cur.fetchall()
        self.assertEquals([(1, d_date, d_date)], result)

    def testDateTime(self):
        """Tests the behaviour of insert/select with mysql/DATETIME <-> python/datetime.datetime"""

        d_date = datetime.datetime(2010, 02, 11, 13, 37, 42)
        d_string = "2010-02-11 13:37:42"

        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'latin-1', use_unicode = True)

        cur = cnn.cursor()

        cur.execute("drop table if exists tbldate")
        cur.execute("create table tbldate (test_id int(11) DEFAULT NULL, test_date datetime DEFAULT NULL, test_date2 datetime DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cur.execute("insert into tbldate (test_id, test_date, test_date2) values (%s, '" + d_string + "', %s)", (1, d_date))

        # Make sure our insert was correct
        cur.execute("select test_id from tbldate where test_date = test_date2")
        result = cur.fetchall()
        self.assertEquals([(1, )], result)

        # Make sure select gets the right value back
        cur.execute("select test_id, test_date, test_date2 from tbldate where test_date = test_date2")
        result = cur.fetchall()
        self.assertEquals([(1, d_date, d_date)], result)

    def testZeroDates(self):
        """Tests the behaviour of zero dates"""

        zero_datetime = "0000-00-00 00:00:00" 
        zero_date = "0000-00-00"


        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'latin-1', use_unicode = True)

        cur = cnn.cursor()

        cur.execute("drop table if exists tbldate")
        cur.execute("create table tbldate (test_id int(11) DEFAULT NULL, test_date date DEFAULT NULL, test_datetime datetime DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1")

        cur.execute("insert into tbldate (test_id, test_date, test_datetime) values (%s, %s, %s)", (1, zero_date, zero_datetime))

        # Make sure we get None-values back
        cur.execute("select test_id, test_date, test_datetime from tbldate where test_id = 1")
        result = cur.fetchall()
        self.assertEquals([(1, None, None)], result)

    def testUnicodeUTF8(self):
        peacesign_unicode = u"\u262e"
        peacesign_utf8 = "\xe2\x98\xae"

        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'utf-8', use_unicode = True)

        cur = cnn.cursor()
        cur.execute("drop table if exists tblutf")
        cur.execute("create table tblutf (test_id int(11) DEFAULT NULL, test_string VARCHAR(32) DEFAULT NULL) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        cur.execute("insert into tblutf (test_id, test_string) values (%s, %s)", (1, peacesign_unicode)) # This should be encoded in utf8
        cur.execute("insert into tblutf (test_id, test_string) values (%s, %s)", (2, peacesign_utf8))

        cur.execute("select test_id, test_string from tblutf")
        result = cur.fetchall()

        # We expect unicode strings back
        self.assertEquals([(1, peacesign_unicode), (2, peacesign_unicode)], result)

    def testCharsets(self):
        aumlaut_unicode = u"\u00e4"
        aumlaut_utf8 = "\xc3\xa4"
        aumlaut_latin1 = "\xe4"


        cnn = dbapi.connect(host = DB_HOST, user = DB_USER,
                            password = DB_PASSWD, db = DB_DB,
                            charset = 'utf8', use_unicode = True)

        cur = cnn.cursor()
        cur.execute("drop table if exists tblutf")
        cur.execute("create table tblutf (test_mode VARCHAR(32) DEFAULT NULL, test_utf VARCHAR(32) DEFAULT NULL, test_latin1 VARCHAR(32)) ENGINE=MyISAM DEFAULT CHARSET=utf8")

        # We insert the same character using two different encodings
        cur.execute("set names utf8")
        cur.execute("insert into tblutf (test_mode, test_utf, test_latin1) values ('utf8', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")
        
        cur.execute("set names latin1")
        cur.execute("insert into tblutf (test_mode, test_utf, test_latin1) values ('latin1', _utf8'" + aumlaut_utf8 + "', _latin1'" + aumlaut_latin1 + "')")

        # We expect the driver to always give us unicode strings back
        expected = [(u"utf8", aumlaut_unicode, aumlaut_unicode), (u"latin1", aumlaut_unicode, aumlaut_unicode)]

        # Fetch and test with different charsets
        for charset in ("latin1", "utf8", "cp1250"):
            cur.execute("set names " + charset)
            cur.execute("select test_mode, test_utf, test_latin1 from tblutf")
            result = cur.fetchall()
            self.assertEquals(result, expected)





if __name__ == '__main__':
    unittest.main()



