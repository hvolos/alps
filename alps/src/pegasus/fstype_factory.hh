/* 
 * (c) Copyright 2016 Hewlett Packard Enterprise Development LP
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _ALPS_PEGASUS_FSTYPE_FACTORY_HH_
#define _ALPS_PEGASUS_FSTYPE_FACTORY_HH_

#include "common/assert_nd.hh"
#include "common/debug.hh"
#include "common/error_code.hh"
#include "common/os.hh"
#include "pegasus/pegasus_options.hh"

namespace alps {

/**
 * @brief A factory class for constructing objects based on the type of 
 * the underlying file system.
 */
template<typename ObjectType>
class FileSystemTypeFactory {
public:
    typedef ObjectType* (*ConstructCallback)(const boost::filesystem::path& pathname, const PegasusOptions& pegasus_options);

public:
    FileSystemTypeFactory(const PegasusOptions& pegasus_options);
    ErrorCode construct(const boost::filesystem::path& pathname, ObjectType** object);
    ErrorCode register_fstype(const std::string& fstype, ConstructCallback);

private:
    typedef std::map<std::string, ConstructCallback> ConstructCallbackMap;

private:
    std::string fstype(const boost::filesystem::path& pathname);

protected:
    ConstructCallbackMap callbacks_;
    PegasusOptions       pegasus_options_;
};


template<typename ObjectType>
FileSystemTypeFactory<ObjectType>::FileSystemTypeFactory(const PegasusOptions& pegasus_options)
    : pegasus_options_(pegasus_options)
{ }


template<typename ObjectType>
std::string FileSystemTypeFactory<ObjectType>::fstype(const boost::filesystem::path& pathname)
{
    return os_fstype(pathname.c_str());
}


template<typename ObjectType>
ErrorCode FileSystemTypeFactory<ObjectType>::construct(const boost::filesystem::path& pathname, ObjectType** object)
{
    boost::filesystem::path parent_path = pathname.parent_path();
    if (!boost::filesystem::exists(parent_path)) {
        LOG(error) << "Directory " << parent_path << " does not exist.";
        return kErrorCodeFsNoDirectory;
    }

    std::string fst = fstype(pathname);

    ConstructCallback ccb;
    typename ConstructCallbackMap::const_iterator it = callbacks_.find(fst);
    typename ConstructCallbackMap::const_iterator it_any = callbacks_.find("any");
    if (it == callbacks_.end() && it_any == callbacks_.end()) {
        LOG(error) << "Unknown file system type " << fst
                   << ". Check whether file system is known to os_fstype.";
        return kErrorCodeFsUnknownFSType;
    } else {
        if (it == callbacks_.end()) {
            ccb = it_any->second;
        } else {
            ccb = it->second;
        }
    }
    ObjectType* obj;
    if ((obj = ccb(pathname, pegasus_options_)) == NULL) {
        return kErrorCodeOutofmemory;
    }
    *object = obj;
    return kErrorCodeOk;
}


template<typename ObjectType>
ErrorCode FileSystemTypeFactory<ObjectType>::register_fstype(const std::string& fstype, ConstructCallback ccb)
{
    callbacks_[fstype] = ccb;
    return kErrorCodeOk;
}

} // namespace alps

#endif // _ALPS_PEGASUS_FSTYPE_FACTORY_HH_
