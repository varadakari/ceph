#include "FInj.h"
#include "osd/OSDFInj.h"
#include "osd/OSDFInjList.h"
#include "string.h"
#include <pthread.h>
#include <iostream>
#include <vector>
#include <unistd.h>
#include <stdlib.h>
#include "include/str_list.h"

list<FInj*>* FInj::FInjList=NULL;

FInj::FInj(const int num_arg,const char* filename_arg):
num(num_arg),filename(filename_arg) {
  enabled=false;
  if(FInjList == NULL)
    FInjList= new list<FInj*>;
  FInj::FInjList->push_back(this);
}

FInj::~FInj() {
  FInj::FInjList->remove(this);
  if (FInjList->empty()) {
    delete FInjList;
    FInjList = NULL;
  }
}

bool
FInj::fire() {
  if(enabled) {
    if (sleep_enabled) {
      sleep(duration);
    }  
    if (count == -1) {
      return true;
    } else if (--count == 0) {
      enabled = false;
    }
    return true;
  }
  return false;
}

static void
reset_all() {
  if (FInj::FInjList==NULL)
    return;
  FInj* iot;
  list<FInj*>::iterator pos = FInj::FInjList->begin();
  for (; pos != FInj::FInjList->end(); pos++) {
    iot = dynamic_cast<FInj *>(*pos);
    iot->reset_enabled();
    iot->reset_count();
    iot->reset_sleep_duration();
  }
}

static void
set_specific(const int i, const int count, const int sduration) {
  if (FInj::FInjList==NULL)
    return;
  FInj* iot;
  list<FInj*>::iterator pos = FInj::FInjList->begin();
  for (; pos != FInj::FInjList->end(); pos++) {
    iot = dynamic_cast<FInj *>(*pos);
    if (iot->num == i)  {
      iot->set_enabled();
      iot->set_count(count);
      iot->set_sleep_duration(sduration);
    }
  }
}

static void
reset_enabled(const int i) {
  if (FInj::FInjList==NULL)
    return;
  FInj* iot;
  list<FInj*>::iterator pos = FInj::FInjList->begin();
  for (; pos != FInj::FInjList->end(); pos++) {
    iot = dynamic_cast<FInj *>(*pos);
    if (iot->num == i)  {
      iot->reset_enabled();
      iot->reset_count();
      iot->reset_sleep_duration();
    }
  }
}

static void
dump_all(ostream &out) {
  if (FInj::FInjList==NULL)
    return;
  FInj* iot;
  list<FInj*>::iterator pos = FInj::FInjList->begin();
  for (; pos != FInj::FInjList->end(); pos++) {
    iot = dynamic_cast<FInj *>(*pos);
    out << *iot;
  }
}

static void
dump_enabled(ostream &out) {
  if (FInj::FInjList==NULL)
    return;
  FInj* iot;
  list<FInj*>::iterator pos = FInj::FInjList->begin();
  for (; pos != FInj::FInjList->end(); pos++) {
    iot = dynamic_cast<FInj *>(*pos);
    if (iot->get_enabled())
      out << *iot;
  }
}

void ceph_handle_finj_cmd(std::vector<std::string>& cmd, ostream& out)
{
  if (cmd.size() == 0) {
    return;
  }


  if (cmd.size() == 1 && cmd[0] == "dump") {
    dump_all(out);
  } else if (cmd.size() == 1 && cmd[0] == "reset") {
    reset_all();
  } else if (cmd.size() > 1 && cmd[0] == "dump") {
    if (cmd[1] == "all") {
      dump_all(out);
    } else if (cmd[1] == "enabled") {
      dump_enabled(out);
    } else {
      out <<"Unknown command";
    }
  } else if (cmd.size() > 1 && cmd[0] == "reset") {
    cmd.erase(cmd.begin());
    if (cmd[0] == "all") {
      reset_all();
    } else if(cmd[0] == "trigger") {
      int num = atoi(cmd[1].c_str());
      reset_enabled(num);
    } else {
      out <<"Unknown command";
    }
  } else if (cmd.size() > 1 && cmd[0] == "set") {
    int num = 0, sduration = 0, count = -1;
    cmd.erase(cmd.begin());
    while(cmd.size() > 0) {
      if (cmd[0] == "trigger") {
        num = atoi(cmd[1].c_str());
      } else if (cmd[0] == "count") {
        count = atoi(cmd[1].c_str());
      } else if (cmd[0] == "sleep") {
        sduration = atoi(cmd[1].c_str());
      }
      cmd.erase(cmd.begin());
      cmd.erase(cmd.begin());
    }
    set_specific(num, count, sduration);
  }

}

void ceph_handle_finj_cmd(cmdmap_t &cmdmap, ostream& out)
{
  string cmd_string;
  for (map<string,cmd_vartype>::const_iterator p = cmdmap.begin();
       p != cmdmap.end(); ++p) {
    if (p->first == "prefix")
      continue;
    if (p->first == "caps")
	continue;
    
    cmd_string = cmd_vartype_stringify(p->second);
  }
  cmd_string.erase(0, cmd_string.find_first_not_of('['));
  cmd_string.erase(cmd_string.find_last_not_of(']')+1); 
  vector<string> FInjcmd_vec;
  get_str_vec(cmd_string, FInjcmd_vec);
  ceph_handle_finj_cmd(FInjcmd_vec, out);
}

ostream& operator<<(ostream& out, FInj& iot)
{
  out<<"FInj num = "<< iot.num << "\tFunc = "<< iot.filename<< \
       "\t Enabled: "<< iot.get_enabled() <<"\t Count: "<< iot.get_count()<<\
       "\t Sleep duration: "<<iot.get_sleep_duration()<<"\n";
  return out;
}
