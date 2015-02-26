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


/**
 * @file
 *
 * @brief Wrapper around boost::programm_options and config parser which
 * consolidate command-line arguments, enviroment variables and config options.
 *
 * @author Artyom Smirnov <smirnoffjr@gmail.com>
 */

#include <string>
#include <stdlib.h>
#include <fstream>
#include <errno.h>

#include <boost/program_options.hpp>
#include <boost/program_options/value_semantic.hpp>
#include <boost/program_options/errors.hpp>
#include <boost/format.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

#include "lib_json/json.h"

#include "system/Config.h"
#include "system/Exceptions.h"

using namespace std;
using namespace boost;
using namespace boost::program_options;
using namespace boost::algorithm;

namespace scidb
{

static value_semantic* optTypeToValSem(Config::ConfigOptionType optionType);

static void stringToVector(const string& str, vector<string>& strs);

// For iterating through map::pair<int32_t, Config::ConfigOption*> in BOOST_FOREACH
typedef pair<int32_t, Config::ConfigOption*> opt_pair;

Config::ConfigAddOption::ConfigAddOption(Config *owner) :
	_owner(owner)
{
}

Config::ConfigAddOption& Config::ConfigAddOption::operator()(
		int32_t option,
		char shortCmdLineArg,
		const std::string &longCmdLineArg,
		const std::string &configOption,
		const std::string &envVariable,
		ConfigOptionType type,
		const std::string &description,
		const boost::any &value,
		bool required)
{
	_owner->addOption(option, shortCmdLineArg, longCmdLineArg, configOption,
			envVariable, type, description, value, required);
	return *this;
}

Config::ConfigOption::ConfigOption(
		char shortCmdLineArg,
		const std::string &longCmdLineArg,
		const std::string &configOption,
		const std::string &envVariable,
		ConfigOptionType type,
		const std::string &description,
		const boost::any &value,
		bool required) :
	_short(shortCmdLineArg),
	_long(longCmdLineArg),
	_config(configOption),
	_env(envVariable),
	_type(type),
	_required(required),
	_activated(false),
	_description(description)
{
	if (!value.empty())
	{
		setValue(value);
	}
	else
	{
		// If we not have default value but option not required, so
		// throw exception here to avoid getting unsetted option in future.
		//TODO: exception here?
		if (!required)
			assert(0);
	}
}

void Config::ConfigOption::setValue(const boost::any &value)
{
	// Just runtime check of values types which will be stored in boost::any.
	// Exception will be thrown if value type in any not match specified.
	switch(_type)
	{
		case STRING:
			boost::any_cast<std::string>(value);
			break;
		case INTEGER:
			boost::any_cast<int>(value);
			break;
		case REAL:
			boost::any_cast<double>(value);
			break;
		case BOOLEAN:
			boost::any_cast<bool>(value);
			break;
		case STRING_LIST:
			boost::any_cast<vector<string> >(value);
			break;
		default:
			//TODO: Throw scidb's exceptions here?
			assert(false);
	}

	_value = value;
}

Config::ConfigAddOption Config::addOption(
		int32_t option,
		char shortCmdLineArg,
		const std::string &longCmdLineArg,
		const std::string &configOption,
		const std::string &envVariable,
		ConfigOptionType type,
		const std::string &description,
		const boost::any &value,
		bool required)
{
	// For accessing command line arguments, long argument always must be defined
	assert (!(shortCmdLineArg != 0 && longCmdLineArg == ""));

	_longArgToOption[longCmdLineArg] = option;

	_values[option] = new Config::ConfigOption(shortCmdLineArg,
			longCmdLineArg, configOption, envVariable, type, description, value, required);

	return ConfigAddOption(this);
}

std::string Config::toString()
{
    stringstream ss;
    BOOST_FOREACH(opt_pair p, _values)
    {
        ConfigOption *opt = p.second;
        assert(opt);
        ss << opt->_long << " : " << optionValueToString(opt) << endl;
    }
    return ss.str();
}

void Config::parse(int argc, char **argv, const char* configFileName)
{
	_configFileName = configFileName;

	/*
	 * Loading environment variables
	 */
	BOOST_FOREACH(opt_pair p, _values)
	{
		ConfigOption *opt = p.second;
		if (opt->_env != "")
		{
			char *env = getenv(opt->_env.c_str());
			if (env != NULL)
			{
				switch (opt->_type)
				{
					case Config::BOOLEAN:
						opt->_value = lexical_cast<bool>(env);
						break;
					case Config::STRING:
						opt->_value = lexical_cast<string>(env);
						break;
					case Config::INTEGER:
						opt->_value = lexical_cast<int>(env);
						break;
					case Config::REAL:
						opt->_value = lexical_cast<double>(env);
						break;
					case Config::STRING_LIST:
					{
						vector<string> strs;
						stringToVector(env, strs);
						opt->_value = strs;
						break;
					}
				}
				
				opt->_activated = true;

				BOOST_FOREACH(void (*hook)(int32_t), _hooks)
				{
					hook(p.first);
				}
			}
		}
	}

	/*
	 * Parsing command line arguments
	 */
	options_description argsDesc;

	BOOST_FOREACH(opt_pair p, _values)
	{
		ConfigOption *opt = p.second;

		if (opt->_long != "")
		{
			string arg;
			if (opt->_short)
			{
				arg = str(format("%s,%c") % opt->_long % opt->_short);
			}
			else
			{
				arg = opt->_long;
			}
	
			switch(opt->_type)
			{
				case Config::BOOLEAN:
                  argsDesc.add_options()(arg.c_str(), opt->_description.c_str());
                    break;
				default:
					argsDesc.add_options()
							(arg.c_str(), optTypeToValSem(opt->_type), opt->_description.c_str());
			}
		}
	}

	stringstream desrcStream;
	desrcStream << argsDesc;
	_description = desrcStream.str();
	
	variables_map cmdLineArgs;
	try
	{
		store(parse_command_line(argc, argv, argsDesc), cmdLineArgs);
	}
	catch (const boost::program_options::error &e)
	{
		cerr << "Error during options parsing: " << e.what() << ". Use --help option for details." << endl;
		exit(1);
	}
	catch (const std::exception &e)
	{
		cerr << "Unknown exception during options parsing: " << e.what() << endl;
		exit(1);
	}
	catch (...)
	{
		cerr << "Unknown exception during options parsing" << endl;
		exit(1);
	}

	notify(cmdLineArgs);

	BOOST_FOREACH(opt_pair p, _values)
	{
		ConfigOption *opt = p.second;

		if (cmdLineArgs.count(opt->_long))
		{
			switch (opt->_type)
			{
				case Config::BOOLEAN:
					opt->_value = !boost::any_cast<bool>(opt->_value);
					break;
				case Config::STRING:
					opt->_value = cmdLineArgs[opt->_long].as<string>();
					break;
				case Config::INTEGER:
					opt->_value = cmdLineArgs[opt->_long].as<int>();
					break;
				case Config::REAL:
					opt->_value = cmdLineArgs[opt->_long].as<double>();
					break;
				case Config::STRING_LIST:
					opt->_value = cmdLineArgs[opt->_long].as<vector<string> >();
			}

			opt->_activated = true;
			
			BOOST_FOREACH(void (*hook)(int32_t), _hooks)
			{
				hook(p.first);
			}
		}
	}

	/*
	 * Parsing config file. Though config file parsed after getting environment
	 * variables and command line arguments it not overwrite already setted values
	 * because config has lowest priority.
	 */
	if (_configFileName != "")
	{
        Json::Value root;
        Json::Reader reader;
        ifstream ifile(_configFileName.c_str());

        if (!ifile.is_open())
        {
            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_CANT_OPEN_FILE) << _configFileName << errno;
        }

        string str((istreambuf_iterator<char>(ifile)), istreambuf_iterator<char>());
        trim(str);
        if (!("" == str))
        {
            ifile.seekg(0, ios::beg);
            const bool parsed = reader.parse(ifile, root);
            ifile.close();
            if (parsed)
            {
                //Dumb nested loops search items from config file in defined Config options
                //if some not exists in Config we fail with error.
                BOOST_FOREACH(const string &member, root.getMemberNames())
                {
                    bool found = false;
                    BOOST_FOREACH(const opt_pair &p, _values)
                    {
                        if (p.second->_config == member)
                        {
                            found = true;
                            break;
                        }
                    }

                    if (!found)
                    {
                        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_CONFIG_OPTION) << member;
                    }
                }


                BOOST_FOREACH(opt_pair p, _values)
                {
                    ConfigOption *opt = p.second;

                    if (!opt->_activated)
                    {
                        try
                        {
                            switch (opt->_type)
                            {
                                case Config::BOOLEAN:
                                {
                                    if (root.isMember(opt->_config)) {
                                        opt->_value = bool(root[opt->_config].asBool());
                                        opt->_activated = true;
                                    }
                                    break;
                                }
                                case Config::STRING:
                                {
                                    if (root.isMember(opt->_config)) {
                                        string val = root[opt->_config].asString();
                                        opt->_value = val;
                                        opt->_activated = true;
                                    }
                                    break;
                                }
                                case Config::INTEGER:
                                {
                                    if (root.isMember(opt->_config)) {
                                        opt->_value = int(root[opt->_config].asInt());
                                        opt->_activated = true;
                                    }
                                    break;
                                }
                                case Config::REAL:
                                {
                                    if (root.isMember(opt->_config)) {
                                        opt->_value = double(root[opt->_config].asDouble());
                                        opt->_activated = true;
                                    }
                                    break;
                                }
                                case Config::STRING_LIST:
                                {
                                    if (root.isMember(opt->_config)) {
                                        vector<string> strs;
                                        const Json::Value lst = root[opt->_config];
                                        for (unsigned int i = 0; i < lst.size(); i++) {
                                            strs.push_back(lst[i].asString());
                                        }
                                        opt->_value = strs;
                                        opt->_activated = true;
                                    }
                                    break;
                                }
                            }
                        }
                        catch(const std::exception &e)
                        {
                            throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_ERROR_NEAR_CONFIG_OPTION) << e.what() << opt->_config;
                        }

                        if (opt->_activated) {
                            BOOST_FOREACH(void (*hook)(int32_t), _hooks)
                            {
                                hook(p.first);
                            }
                        }
                    }
                }
            }
            else
            {
                throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_ERROR_IN_CONFIGURATION_FILE) << reader.getFormatedErrorMessages();
            }
        }
        else
        {
            ifile.close();
        }
	}
	
	BOOST_FOREACH(opt_pair p, _values)
	{
		ConfigOption *opt = p.second;
		if ((opt->_required && !opt->_activated) 
			|| (!opt->_required && opt->_value.empty()))
		{
			//FIXME: Replace with scidb exception
			stringstream ss;
			ss << "One of program options required but value not set or value is empty. "
				  "You can set this option with next way(s):\n";
			if (opt->_short != 0 || opt->_long != "")
			{
				ss << "* Through command line argument ";
				if (opt->_short)
					ss << "-" << opt->_short;
				
				if (opt->_long != "")
				{
					if (opt->_short != 0)
						ss << " (";
					ss << "--" << opt->_long;
					if (opt->_short != 0)
						ss << ")";
				}
				ss << endl;
			}
			
			if (opt->_env != "")
			{
				ss << "* Through environment variable '" << opt->_env << "'" << endl;
			}

			if (opt->_config != "")
			{
				ss << "* Through config variable '" << opt->_config << "'" << endl;
			}

			cerr << ss.str();
			exit(1);
		}
	}
}

Config::~Config()
{
	for(std::map<int32_t, ConfigOption*>::iterator it = _values.begin();
			it != _values.end(); ++it)
	{
		delete it->second;
	}
}


void Config::addHook(void (*hook)(int32_t))
{
	_hooks.push_back(hook);
}

//TODO: Will be good to support more then one config file for loading
//e.g system configs from /etc and user config from ~/.config/ for different
//utilites
void Config::setConfigFileName(const std::string& configFileName)
{
	_configFileName = configFileName;
}

const std::string& Config::getDescription() const
{
	return _description;
}

const std::string& Config::getConfigFileName() const
{
	return _configFileName;
}

bool Config::optionActivated(int32_t option)
{
	assert(_values[option]);
	return _values[option]->_activated;
}

void Config::setOption(int32_t option, const boost::any &value)
{
	assert(_values[option]);
	_values[option]->setValue(value);
}


std::string Config::setOptionValue(std::string const& name, std::string const& newValue)
{
    std::map<std::string, int32_t>::const_iterator i = _longArgToOption.find(name);
    if (i == _longArgToOption.end())
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_CONFIG_OPTION) << name;
    ConfigOption *opt = _values[i->second];
    std::string oldValue;
    switch (opt->_type)
    {
      case Config::BOOLEAN:
        oldValue = boost::lexical_cast<std::string>(boost::any_cast<bool>(opt->_value));
        opt->_value = boost::lexical_cast<bool>(newValue);
        break;
      case Config::STRING:
        oldValue = boost::any_cast<std::string>(opt->_value);
        opt->_value = newValue;
        break;
      case Config::INTEGER:
        oldValue = boost::lexical_cast<std::string>(boost::any_cast<int>(opt->_value));
        opt->_value = boost::lexical_cast<int>(newValue);
        break;
      case Config::REAL:
        oldValue = boost::lexical_cast<std::string>(boost::any_cast<double>(opt->_value));
        opt->_value = boost::lexical_cast<double>(newValue);
        break;
      default:
        assert(0);
    }
    return oldValue;
}

std::string Config::getOptionValue(std::string const& name)
{
    std::map<std::string, int32_t>::const_iterator i = _longArgToOption.find(name);
    if (i == _longArgToOption.end())
        throw USER_EXCEPTION(SCIDB_SE_CONFIG, SCIDB_LE_UNKNOWN_CONFIG_OPTION) << name;
    ConfigOption *opt = _values[i->second];
    return optionValueToString(opt);
}

std::string Config::optionValueToString(ConfigOption *opt)
{
    assert(opt);
    switch (opt->_type)
    {
      case Config::BOOLEAN:
        return boost::lexical_cast<std::string>(boost::any_cast<bool>(opt->_value));
        break;
      case Config::STRING:
        return boost::any_cast<std::string>(opt->_value);
      case Config::INTEGER:
        return boost::lexical_cast<std::string>(boost::any_cast<int>(opt->_value));
      case Config::REAL:
        return boost::lexical_cast<std::string>(boost::any_cast<double>(opt->_value));
      default:
        assert(0);
    }
    return "";
}

static value_semantic* optTypeToValSem(Config::ConfigOptionType optionType)
{
	switch (optionType)
	{
		case Config::BOOLEAN:
			return value<bool>();
		case Config::STRING:
			return value<string>();
		case Config::INTEGER:
			return value<int>();
		case Config::REAL:
			return value<double>();
		case Config::STRING_LIST:
			return value<vector<string> >()->multitoken();
		default:
			assert(0);
	}
}

//TODO: Maybe replace with something more complicated? (e.g. little boost::spirit parser)
static void stringToVector(const string& str, vector<string>& strs)
{
    split(strs, str, is_any_of(":"));
}


} // namespace common
	
