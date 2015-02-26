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

#ifndef CONFIG_H_
#define CONFIG_H_

#include <string>
#include <boost/any.hpp>
#include <assert.h>
#include <map>
#include <stdint.h>
#include <iostream>
#include <vector>

#include "util/Singleton.h"

namespace scidb
{

class Config: public Singleton<Config>
{
public:
	typedef enum
	{
            STRING,
            INTEGER,
            REAL,
            BOOLEAN,
            STRING_LIST
	} ConfigOptionType;

	class ConfigAddOption
	{
	public:
		ConfigAddOption(Config *owner);

		ConfigAddOption& operator()(
				int32_t option,
				char shortCmdLineArg,
				const std::string &longCmdLineArg,
				const std::string &configOption,
				const std::string &envVariable,
				ConfigOptionType type,
				const std::string &description = "",
				const boost::any &value = boost::any(),
				bool required = true);

	private:
		Config *_owner;
	};

	class ConfigOption
	{
	public:
		ConfigOption(
				char shortCmdLineArg,
				const std::string &longCmdLineArg,
				const std::string &configOption,
				const std::string &envVariable,
				ConfigOptionType type,
				const std::string &description,
				const boost::any &value,
				bool required = true);

		void setValue(const boost::any &value);
		
	private:
		char _short;
		std::string _long;
		std::string _config;
		std::string _env;
		ConfigOptionType _type;
		boost::any _value;
		bool _required;
		bool _activated;
		std::string _description;

		friend class Config;
	};

	ConfigAddOption addOption(
			int32_t option,
			char shortCmdLineArg,
			const std::string &longCmdLineArg,
			const std::string &configOption,
			const std::string &envVariable,
			ConfigOptionType type,
			const std::string &description = "",
			const boost::any &value = boost::any(),
			bool required = true);

	void addHook(void (*hook)(int32_t));

	void parse(int argc, char **argv, const char* configFileName);

    template<class T>
    const T& getOption(int32_t option)
    {
    	assert(_values[option]);
        return boost::any_cast<const T&>(_values[option]->_value);
    }
	
	/**
	 * With this function it able to reinit config file path during parsing
	 * command line arguments or environment variables inside config hooks before
	 * opening default config.
	 *
	 * @param configFileName Path to config file
	 */
	void setConfigFileName(const std::string& configFileName);

	const std::string& getDescription() const;
	
	const std::string& getConfigFileName() const;

	bool optionActivated(int32_t option);

    void setOption(int32_t option, const boost::any &value);

    std::string setOptionValue(std::string const& name, std::string const& value);

    std::string getOptionValue(std::string const& name);

    std::string toString();

private:
	~Config();

        std::string optionValueToString(ConfigOption *opt);

	std::map<int32_t, ConfigOption*> _values;

	std::map<std::string, int32_t> _longArgToOption;

	std::vector<void (*)(int32_t)> _hooks;

	std::string _configFileName;

	std::string _description;
	
	friend class Singleton<Config>;	
};

}
#endif /* CONFIG_H_ */
