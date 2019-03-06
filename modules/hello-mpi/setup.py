from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

srcs = ["hello_mpi.pyx", "hello_mpi_c.cpp"]
extensions = [Extension("hello_mpi",
                        sources=srcs,
                        language="c++",
                        libraries=["mpi"])]
setup(ext_modules = cythonize(extensions))
