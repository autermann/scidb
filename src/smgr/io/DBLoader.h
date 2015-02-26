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

/*
 * DBArray.h
 *
 *  Created on: 19.02.2010
 *      Author: knizhnik@garret.ru
 *      Description: Database loader from text format
 */

#ifndef DBLOADER_H_
#define DBLOADER_H_

#include <stddef.h>
#include <string>

#include "array/Array.h"

namespace scidb
{
    class DBLoader
    {
      public:
        static int defaultPrecision;

        /**
         * Save data from in text format in specified file
         * @param arrayName name of the array which data will be saved
         * @param file path to the exported data file
         * @param format output format: csv, csv+, sparse, auto
         * @param query doing the save
         * @return number of saved tuples
         */
       static uint64_t save(std::string const& arrayName, std::string const& file,
                            const boost::shared_ptr<Query>& query,
                            std::string const& format = "auto", bool append = false);

        /**
         * Save data from in text format in specified file
         * @param array array to be saved
         * @param file path to the exported data file
         * @param format output format: csv, csv+, sparse, auto
         * @return number of saved tuples
         */
        static uint64_t save(Array const& array, std::string const& file,
                             const boost::shared_ptr<Query>& query,
                             std::string const& format = "auto", bool append = false);

    };
}

#endif
