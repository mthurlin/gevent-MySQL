from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

VERSION = '0.0.1'

setup(
    name = "gevent-MySQL",
    version = VERSION,
    license = "New BSD",
    description = "A gevent (http://www.gevent.org) adaption of the asynchronous MySQL driver from the Concurrence framework (http://opensource.hyves.org/concurrence)",
    cmdclass = {"build_ext": build_ext},
    package_dir = {'':'lib'},
    packages = ['geventmysql'],
    ext_modules = [Extension("geventmysql._mysql", ["lib/geventmysql/geventmysql._mysql.pyx"])]
)