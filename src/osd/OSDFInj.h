#ifndef _OSD_FInj_HEADER_
#define _OSD_FInj_HEADER_

#include "common/FInj.h"


#define FINJ(n, call) ({extern OSDFInj osd_finj_iot##n; osd_finj_iot##n .fire()?(-1):(call);})

class OSDFInj : public FInj {
// nothing to implement now
public:
  OSDFInj(int num_arg, const char* filename_arg):FInj(num_arg, filename_arg) { }
  virtual ~OSDFInj() { }
};

#endif
