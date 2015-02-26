#ifndef GROUPER_H
#define GROUPER_H

#include "mysqlwrapper.h"
#include <log4cxx/logger.h>
#include "array/MemArray.h"
#include "array/Metadata.h"
#include "cookgroup.h"
  
using namespace scidb;

class Grouper{
public:
  void loadGroup(const std::vector<ObsPos> &allObs,const std::vector<Image> &allImages,float D2, int T);
  void storeGroup(boost::shared_ptr<MemArray> output);
  int getSize(){return groups.size();};
  int getGroupSize(){return groups.size();};
private:
  std::vector<ObsPos> allObs;
  int allObsCount;
  int binarySearchObsId (int obsid) const;

  boost::shared_ptr<ArrayIterator> _oidIterator;
  boost::shared_ptr<ChunkIterator> _oidChunkit;

  boost::shared_ptr<ArrayIterator> _xIterator;
  boost::shared_ptr<ChunkIterator> _xChunkit;

  boost::shared_ptr<ArrayIterator> _yIterator;
  boost::shared_ptr<ChunkIterator> _yChunkit;

  std::map<int, std::vector<int> > groups;
};

#endif // GROUPER_H
