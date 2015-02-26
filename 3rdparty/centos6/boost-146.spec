%define scidb_boost scidb-boost-SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR
%define scidb_path  /opt/scidb/SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR
Name: %{scidb_boost}
Summary: The free peer-reviewed portable C++ source libraries
Version: 1.46.1
Release: SCIDB_VERSION_PATCH
License: Boost
URL: http://www.boost.org/
Group: System Environment/Libraries
Source: http://sourceforge.net/projects/boost/files/boost/1.46.1/boost_1_46_1.tar.bz2

# Patches for fix warning under CentOS 6
# https://svn.boost.org/trac/boost/ticket/4276
Patch0: boost-map_iterator.patch
# https://svn.boost.org/trac/boost/ticket/5416
Patch1: boost-serializationExportWarningPatch.patch

%define srcdir boost_1_46_1
%define _docdir %{scidb_path}/doc
%define _libdir %{scidb_path}/lib
Requires: %{scidb_boost}-date-time = %{version}-%{release}
Requires: %{scidb_boost}-filesystem = %{version}-%{release}
Requires: %{scidb_boost}-graph = %{version}-%{release}
Requires: %{scidb_boost}-iostreams = %{version}-%{release}
Requires: %{scidb_boost}-program-options = %{version}-%{release}
Requires: %{scidb_boost}-python = %{version}-%{release}
Requires: %{scidb_boost}-regex = %{version}-%{release}
Requires: %{scidb_boost}-serialization = %{version}-%{release}
Requires: %{scidb_boost}-signals = %{version}-%{release}
Requires: %{scidb_boost}-system = %{version}-%{release}
Requires: %{scidb_boost}-test = %{version}-%{release}
Requires: %{scidb_boost}-thread = %{version}-%{release}
Requires: %{scidb_boost}-wave = %{version}-%{release}
Requires: %{scidb_boost}-random = %{version}-%{release}
Requires: %{scidb_boost}-math = %{version}-%{release}

BuildRequires: libstdc++-devel
BuildRequires: bzip2-libs
BuildRequires: bzip2-devel
BuildRequires: zlib-devel
BuildRequires: python-devel
BuildRequires: libicu-devel
BuildRequires: chrpath

%description
Boost provides free peer-reviewed portable C++ source libraries.  The
emphasis is on libraries which work well with the C++ Standard
Library, in the hopes of establishing "existing practice" for
extensions and providing reference implementations so that the Boost
libraries are suitable for eventual standardization. (Some of the
libraries have already been proposed for inclusion in the C++
Standards Committee's upcoming C++ Standard Library Technical Report.)

%package date-time
Summary: Runtime component of boost date-time library
Group: System Environment/Libraries
%description date-time
Runtime support for Boost Date Time, set of date-time libraries based
on generic programming concepts.

%package filesystem
Summary: Runtime component of boost filesystem library
Group: System Environment/Libraries
%description filesystem
Runtime support for the Boost Filesystem Library, which provides
portable facilities to query and manipulate paths, files, and
directories.

%package graph
Summary: Runtime component of boost graph library
Group: System Environment/Libraries
%description graph
Runtime support for the BGL graph library.  BGL interface and graph
components are generic, in the same sense as the the Standard Template
Library (STL).

%package iostreams
Summary: Runtime component of boost iostreams library
Group: System Environment/Libraries
%description iostreams
Runtime support for Boost.IOStreams, a framework for defining streams,
stream buffers and i/o filters.

%package math
Summary: Stub that used to contain boost math library
Group: System Environment/Libraries
%description math
This package is a stub that used to contain runtime component of boost
math library.  Now that boost math library is header-only, this
package is empty.  It's kept around only so that during yum-assisted
update, old libraries from %{scidb_boost}-math package aren't left around.

%package program-options
Summary:  Runtime component of boost program_options library
Group: System Environment/Libraries
%description program-options
Runtime support of boost program options library, which allows program
developers to obtain (name, value) pairs from the user, via
conventional methods such as command line and configuration file.

%package python
Summary: Runtime component of boost python library
Group: System Environment/Libraries
%description python
The Boost Python Library is a framework for interfacing Python and
C++. It allows you to quickly and seamlessly expose C++ classes
functions and objects to Python, and vice versa, using no special
tools -- just your C++ compiler.  This package contains runtime
support for Boost Python Library.

%package regex
Summary: Runtime component of boost regular expression library
Group: System Environment/Libraries
%description regex
Runtime support for boost regular expression library.

%package serialization
Summary: Runtime component of boost serialization library
Group: System Environment/Libraries
%description serialization
Runtime support for serialization for persistence and marshaling.

%package signals
Summary: Runtime component of boost signals and slots library
Group: System Environment/Libraries
%description signals
Runtime support for managed signals & slots callback implementation.

%package system
Summary: Runtime component of boost system support library
Group: System Environment/Libraries
%description system
Runtime component of Boost operating system support library, including
the diagnostics support that will be part of the C++0x standard
library.

%package test
Summary: Runtime component of boost test library
Group: System Environment/Libraries
%description test
Runtime support for simple program testing, full unit testing, and for
program execution monitoring.

%package thread
Summary: Runtime component of boost thread library
Group: System Environment/Libraries
%description thread
Runtime component Boost.Thread library, which provides classes and
functions for managing multiple threads of execution, and for
synchronizing data between the threads or providing separate copies of
data specific to individual threads.

%package wave
Summary: Runtime component of boost C99/C++ preprocessing library
Group: System Environment/Libraries
%description wave
Runtime support for the Boost.Wave library, a Standards conformant,
and highly configurable implementation of the mandated C99/C++
preprocessor functionality.

%package random
Summary: Runtime component of boost random library
Group: System Environment/Libraries
%description random
The Boost Random Number Library (Boost.Random for short) provides a
variety of generators and distributions to produce random numbers having
useful properties, such as uniform distribution.

%package devel
Summary: The Boost C++ headers and shared development libraries
Group: Development/Libraries
Requires: %{scidb_boost} = %{version}-%{release}
Provides: %{scidb_boost}-python-devel = %{version}-%{release}
%description devel
Headers and shared object symlinks for the Boost C++ libraries.

%package static
Summary: The Boost C++ static development libraries
Group: Development/Libraries
Requires: %{scidb_boost}-devel = %{version}-%{release}
Obsoletes: %{scidb_boost}-devel-static < 1.34.1-14
Provides: %{scidb_boost}-devel-static = %{version}-%{release}
%description static
Static Boost C++ libraries.

%package doc
Summary: HTML documentation for the Boost C++ libraries
Group: Documentation
BuildArch: noarch
Provides: %{scidb_boost}-python-docs = %{version}-%{release}
%description doc
This package contains the documentation in the HTML format of the Boost C++
libraries. The documentation provides the same content as that on the Boost
web page (http://www.boost.org/doc/libs/1_40_0).

%prep
%setup -q -n %{srcdir}
%patch0 -p1
%patch1 -p1

%build
./bootstrap.sh --prefix=/usr
./bjam --without-mpi %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
cd %{_builddir}/%{srcdir}/
./bjam --prefix=$RPM_BUILD_ROOT/usr install
mkdir -p "$RPM_BUILD_ROOT/%{scidb_path}"
mv "$RPM_BUILD_ROOT/usr/lib" "$RPM_BUILD_ROOT/%{scidb_path}"
mv "$RPM_BUILD_ROOT/usr/include" "$RPM_BUILD_ROOT/%{scidb_path}"

%clean
rm -rf $RPM_BUILD_ROOT

%post date-time -p /sbin/ldconfig

%postun date-time -p /sbin/ldconfig

%post filesystem -p /sbin/ldconfig

%postun filesystem -p /sbin/ldconfig

%post graph -p /sbin/ldconfig

%postun graph -p /sbin/ldconfig

%post iostreams -p /sbin/ldconfig

%postun iostreams -p /sbin/ldconfig

%post program-options -p /sbin/ldconfig

%postun program-options -p /sbin/ldconfig

%post python -p /sbin/ldconfig

%postun python -p /sbin/ldconfig

%post regex -p /sbin/ldconfig

%postun regex -p /sbin/ldconfig

%post serialization -p /sbin/ldconfig

%postun serialization -p /sbin/ldconfig

%post signals -p /sbin/ldconfig

%postun signals -p /sbin/ldconfig

%post system -p /sbin/ldconfig

%postun system -p /sbin/ldconfig

%post test -p /sbin/ldconfig

%postun test -p /sbin/ldconfig

%post thread -p /sbin/ldconfig

%postun thread -p /sbin/ldconfig

%post wave -p /sbin/ldconfig

%postun wave -p /sbin/ldconfig

%post random -p /sbin/ldconfig

%postun random -p /sbin/ldconfig

%files

%files date-time
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_date_time*.so.%{version}

%files filesystem
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_filesystem*.so.%{version}

%files graph
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_graph.so.%{version}

%files iostreams
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_iostreams*.so.%{version}

%files math
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_math*.so.%{version}

%files test
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_prg_exec_monitor*.so.%{version}
%{_libdir}/libboost_unit_test_framework*.so.%{version}

%files program-options
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_program_options*.so.%{version}

%files python
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_python*.so.%{version}

%files regex
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_regex*.so.%{version}

%files serialization
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_serialization*.so.%{version}
%{_libdir}/libboost_wserialization*.so.%{version}

%files signals
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_signals*.so.%{version}

%files system
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_system*.so.%{version}

%files thread
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_thread*.so.%{version}

%files wave
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_wave*.so.%{version}

%files random
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_random*.so.%{version}

%files doc
%defattr(-, root, root, -)
%doc %{_docdir}/scidb-boost-*-%{version}

%files devel
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{scidb_path}/include/boost
%{_libdir}/libboost_*.so

%files static
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/*.a
