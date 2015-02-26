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
 * @file main.cpp
 * @author girish_hilage@persistent.co.in
 */

# include <iostream>

# include "global.h"
# include "harness.h"
# include "interface.h"
# include "Exceptions.h"

using namespace std;
namespace harnessexceptions = scidbtestharness::Exceptions;
int main (int argc, char** argv)
{
	interface::Application *a = new scidbtestharness::SciDBTestHarness;
	int rv;

	try
	{
		if ((rv = a->run (argc, argv, COMMANDLINE)) == FAILURE)
		{
			delete a;
			return EXIT_FAILURE;
		}
	}

	catch (harnessexceptions :: ERROR &e)
	{
		cout << e.what () << endl;
		delete a;
		return EXIT_FAILURE;
	}

    catch (const std::exception& e)
	{
		cout << e.what () << endl;
		delete a;
		return EXIT_FAILURE;
	}

	catch (...)
	{
		cout << "Unhandled Exception caught...\n";
		delete a;
		return EXIT_FAILURE;
	}

	delete a;
	return EXIT_SUCCESS;
}
