from distutils.core import setup
from Cython.Build import cythonize
from distutils.extension import Extension

srcs = ["hello_mpi.pyx", "hello_mpi_c.cpp"]
extensions = [Extension("hello_mpi",
                        sources=srcs,
                        language="c++",
                        libraries=["mpi"],
                        extra_compile_args=["-std=c++11"])]
setup(name="hello_mpi",
      ext_modules = cythonize(extensions))
