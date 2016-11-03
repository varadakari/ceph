#ifndef _FInj_HEADER_
#define _FInj_HEADER_

#include <list>
#include <vector>
#include <string>
#include "common/cmdparse.h"

using namespace std;


class FInj {
public:
  FInj(int n, const char*);
  virtual ~FInj();
  bool is_enabled() {return enabled;};
  bool fire();
  static void reset_all();
  void set_enabled() {
    enabled = true;
  }
  bool get_enabled() {
    return enabled;
  }
  void reset_enabled() {
    enabled = false;
  }
  void set_count(const int i) {
    count = i;
  }

  int get_count() {
    return count;
  }

  void reset_count() {
    count = 0;
  }
  void set_sleep_duration(const int i) {
    duration = i;
    sleep_enabled = true;
  }

  int get_sleep_duration() {
    return duration;
  }

  void reset_sleep_duration() {
    duration = 0;
    sleep_enabled = false;
  }
public:
  int num;
  const char *filename;
  static list<FInj*>* FInjList;
private:
  int count;
  int duration;
  bool enabled;
  bool sleep_enabled;
};

void ceph_handle_finj_cmd(std::vector<std::string>& cmd, ostream& out);
void ceph_handle_finj_cmd(cmdmap_t& cmdmap, ostream& out);
ostream& operator<<(ostream& out, FInj& iot);
#endif
