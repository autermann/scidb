/*
**
* BEGIN_COPYRIGHT
*
* This file is part of SciDB.
* Copyright (C) 2008-2012 SciDB, Inc.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation version 3 of the License.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the GNU General Public License for the complete license terms.
*
* You should have received a copy of the GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/>.
*
* END_COPYRIGHT
*/

#include <unistd.h>
#include <sys/types.h>
#include <iostream>
#include <boost/version.hpp>
#include <boost/interprocess/detail/os_file_functions.hpp>
#include <util/shm/SharedMemoryIpc.h>
#include <util/FileIO.h>

using namespace std;
using namespace boost::interprocess;

namespace scidb
{

SharedMemoryIpc::SharedMemoryIpc(const std::string& name)
: _name(name)
{
    assert(!name.empty());
}

void SharedMemory::create(AccessMode amode)
{
    if (_shm || _region) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    try {
        _shm.reset(new shared_memory_object(create_only, getName().c_str(),
                                            static_cast<boost::interprocess::mode_t>(amode)));
    } catch(boost::interprocess::interprocess_exception &ex) {
        std::cerr << "shm::create error:" << ex.what() << std::endl;
        if (ex.get_error_code() == boost::interprocess::already_exists_error) {
            throw AlreadyExistsException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        if (ex.get_error_code() !=  boost::interprocess::no_error) {
            throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        throw;
    }
}

void SharedMemory::open(AccessMode amode)
{
    if (_shm || _region) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    try {
        _shm.reset(new shared_memory_object(open_only, getName().c_str(),
                                            static_cast<boost::interprocess::mode_t>(amode)));
    } catch(boost::interprocess::interprocess_exception &ex) {
        std::cerr << "shm::open error:" << ex.what() << std::endl;
        if (ex.get_error_code() == boost::interprocess::not_found_error) {
            throw NotFoundException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        if (ex.get_error_code() !=  boost::interprocess::no_error) {
            throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        throw;
    }
}

void SharedMemory::close()
{
    _shm.reset(); // POSIX close(fd)
}

void SharedMemory::unmap()
{
    _region.reset(); // POSIX munmap()
}

void SharedMemory::truncate(uint64_t size, bool force)
{
    if (!_shm) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    if (_region && !force) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    try {
        _region.reset();
        _shm->truncate(size);
    } catch(boost::interprocess::interprocess_exception &ex) {
        std::cerr << "shm::truncate error:" << ex.what() << std::endl;
        if (ex.get_error_code() !=  boost::interprocess::no_error) {
            throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        throw;
    }
}

uint64_t SharedMemory::getSize() const
{
    if (_region) {
        return _region->get_size();
    }
    if (!_shm) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    offset_t n=0;
    bool rc = _shm->get_size(n);
    if (!rc) {
        return 0;
    }
    return static_cast<uint64_t>(n);
}

SharedMemory::AccessMode SharedMemory::getAccessMode() const
{
    if (_region) {
        return static_cast<AccessMode>(_region->get_mode());
    }
    if (!_shm) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    return static_cast<AccessMode>(_shm->get_mode());
}

void* SharedMemory::get()
{
    if (!_region) {
        if (!_shm) {
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
        }
        try {
            // map the whole shared memory in this process
            _region.reset(new mapped_region((*_shm), _shm->get_mode()));
        } catch(boost::interprocess::interprocess_exception &ex) {
            std::cerr << "shm::map error:" << ex.what() << std::endl;
            if (ex.get_error_code() !=  boost::interprocess::no_error) {
                throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
            }
            throw;
        }
    }
    assert(_region);
    return _region->get_address();
}

bool SharedMemory::flush()
{
    if (!_region) {
        return false;
    }
    return _region->flush();
}

bool SharedMemory::remove()
{
    return SharedMemory::remove(getName());
}

SharedMemory::~SharedMemory()
{
    _shm.reset();
    _region.reset();
}

void SharedFile::createFile()
{
    // create the file first
    int fd = scidb::File::openFile(getName().c_str(), (O_RDONLY|O_CREAT|O_EXCL));
    if (fd < 0) {
        int err=errno;
        if (err==EEXIST) {
            throw AlreadyExistsException(err, REL_FILE, __FUNCTION__, __LINE__);
        }
        throw SystemErrorException(err, REL_FILE, __FUNCTION__, __LINE__);
    }
    if (scidb::File::closeFd(fd) != 0) {
        int err=errno;
        throw SystemErrorException(err, REL_FILE, __FUNCTION__, __LINE__);
    }
}

void SharedFile::create(AccessMode amode)
{
    if (_file || _region) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    createFile();
    try {
        _file.reset(new file_mapping(getName().c_str(),
                                     static_cast<boost::interprocess::mode_t>(amode)));
    } catch(boost::interprocess::interprocess_exception &ex) {
        std::cerr << "shared file::create error:" << ex.what() << std::endl;
        if (ex.get_error_code() !=  boost::interprocess::no_error) {
            throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        throw;
    }
}

void SharedFile::open(AccessMode amode)
{
    if (_file || _region) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    try {
        _file.reset(new file_mapping(getName().c_str(),
                                     static_cast<boost::interprocess::mode_t>(amode)));
    } catch(boost::interprocess::interprocess_exception &ex) {
        std::cerr << "shared file::open error:" << ex.what() << std::endl;
        if (ex.get_error_code() == boost::interprocess::not_found_error) {
            throw NotFoundException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        if (ex.get_error_code() !=  boost::interprocess::no_error) {
            throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
        }
        throw;
    }
}

void SharedFile::close()
{
    _file.reset(); // POSIX close(fd)
}
void SharedFile::unmap()
{
    _region.reset(); // POSIX munmap()
}

void SharedFile::truncate(uint64_t size, bool force)
{
    if (!_file) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    if (_region && !force) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    _region.reset();
    if (::truncate(getName().c_str(), size) != 0) {
        int err = errno;
        throw SystemErrorException(err, REL_FILE, __FUNCTION__, __LINE__);
    }
}

uint64_t SharedFile::getSize() const
{
    if (_region) {
        return _region->get_size();
    }
    if (!_file) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    offset_t n=0;
#if (BOOST_VERSION >= 104800)
    file_handle_t fh = ipcdetail::file_handle_from_mapping_handle(_file->get_mapping_handle());
    bool rc = ipcdetail::get_file_size(fh, n);
#else
     file_handle_t fh = detail::file_handle_from_mapping_handle(_file->get_mapping_handle());
     bool rc = detail::get_file_size(fh, n);
#endif
     if (!rc) {
        return 0;
    }
    return static_cast<uint64_t>(n);
}

SharedFile::AccessMode SharedFile::getAccessMode() const
{
    if (_region) {
        return static_cast<AccessMode>(_region->get_mode());
    }
    if (!_file) {
        throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
    }
    return static_cast<AccessMode>(_file->get_mode());
}

void* SharedFile::get()
{
    if (!_region) {
        if (!_file) {
            throw InvalidStateException(REL_FILE, __FUNCTION__, __LINE__);
        }
        try {
            // map the whole shared memory in this process
            _region.reset(new mapped_region((*_file), _file->get_mode()));
            //XXXX TODO tigor: consider close()
        } catch(boost::interprocess::interprocess_exception &ex) {
            std::cerr << "shared file::map error:" << ex.what() << std::endl;
            if (ex.get_error_code() !=  boost::interprocess::no_error) {
                throw SystemErrorException(ex.get_native_error(), REL_FILE, __FUNCTION__, __LINE__);
            }
            throw;
        }
    }
    assert(_region);
    return _region->get_address();
}

bool SharedFile::flush()
{
    if (!_region) {
        return false;
    }
    return _region->flush();
}

bool SharedFile::remove()
{
    return SharedFile::remove(getName().c_str());
}

SharedFile::~SharedFile()
{
    _file.reset();
    _region.reset();
}

}
