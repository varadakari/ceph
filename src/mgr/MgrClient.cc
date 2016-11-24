// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "MgrClient.h"

#include "mgr/MgrContext.h"

#include "msg/Messenger.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrReport.h"
#include "messages/MMgrOpen.h"
#include "messages/MMgrConfigure.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"
#include "messages/MPGStats.h"

#define dout_subsys ceph_subsys_mgrc
#undef dout_prefix
#define dout_prefix *_dout << "mgrc " << __func__ << " "

MgrClient::MgrClient(CephContext *cct_, Messenger *msgr_)
    : Dispatcher(cct_), cct(cct_), msgr(msgr_),
      session(nullptr),
      lock("mgrc"),
      timer(cct_, lock),
      report_callback(nullptr)
{
  assert(cct != nullptr);
}

void MgrClient::init()
{
  Mutex::Locker l(lock);

  assert(msgr != nullptr);

  timer.init();
}

void MgrClient::shutdown()
{
  Mutex::Locker l(lock);

  timer.shutdown();
}

bool MgrClient::ms_dispatch(Message *m)
{
  Mutex::Locker l(lock);

  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(static_cast<MMgrMap*>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_configure(static_cast<MMgrConfigure*>(m));
  case MSG_COMMAND_REPLY:
    if (m->get_source().type() == CEPH_ENTITY_TYPE_MGR) {
      handle_command_reply(static_cast<MCommandReply*>(m));
      return true;
    } else {
      return false;
    }
  default:
    ldout(cct, 30) << "Not handling " << *m << dendl; 
    return false;
  }
}

void MgrClient::reconnect()
{
  assert(lock.is_locked_by_me());

  if (session) {
    ldout(cct, 4) << "Terminating session with "
		  << session->con->get_peer_addr() << dendl;
    session->con->mark_down();
    delete session;
    session = nullptr;
    stats_period = 0;
    if (report_callback != nullptr) {
      timer.cancel_event(report_callback);
      report_callback = nullptr;
    }

    std::vector<ceph_tid_t> erase_cmds;
    auto commands = command_table.get_commands();
    for (const auto &i : commands) {
      // FIXME be nicer, retarget command on new mgr?
      if (i.second.on_finish != nullptr) {
	i.second.on_finish->complete(-ETIMEDOUT);
      }
      erase_cmds.push_back(i.first);
    }
    for (const auto &tid : erase_cmds) {
      command_table.erase(tid);
    }
  }

  if (map.get_available()) {
    ldout(cct, 4) << "Starting new session with " << map.get_active_addr()
		  << dendl;
    entity_inst_t inst;
    inst.addr = map.get_active_addr();
    inst.name = entity_name_t::MGR(map.get_active_gid());

    session = new MgrSessionState();
    session->con = msgr->get_connection(inst);

    // Don't send an open if we're just a client (i.e. doing
    // command-sending, not stats etc)
    if (g_conf && !g_conf->name.is_client()) {
      auto open = new MMgrOpen();
      open->daemon_name = g_conf->name.get_id();
      session->con->send_message(open);
    }

    signal_cond_list(waiting_for_session);
  } else {
    ldout(cct, 4) << "No active mgr available yet" << dendl;
  }
}

bool MgrClient::handle_mgr_map(MMgrMap *m)
{
  assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  map = m->get_map();
  ldout(cct, 4) << "Got map version " << map.epoch << dendl;
  m->put();

  ldout(cct, 4) << "Active mgr is now " << map.get_active_addr() << dendl;

  // Reset session?
  if (session == nullptr || 
      session->con->get_peer_addr() != map.get_active_addr()) {
    reconnect();
  }

  return true;
}

bool MgrClient::ms_handle_reset(Connection *con)
{
  Mutex::Locker l(lock);
  if (session && con == session->con) {
    ldout(cct, 4) << __func__ << " con " << con << dendl;
    reconnect();
    return true;
  }
  return false;
}

bool MgrClient::ms_handle_refused(Connection *con)
{
  // do nothing for now
  return false;
}

void MgrClient::send_report()
{
  assert(lock.is_locked_by_me());
  assert(session);
  report_callback = nullptr;

  auto report = new MMgrReport();
  auto pcc = cct->get_perfcounters_collection();

  pcc->with_counters([this, report](
        const PerfCountersCollection::CounterMap &by_path)
  {
    bool const declared_all = (session->declared.size() == by_path.size());

    if (!declared_all) {
      for (const auto &i : by_path) {
        auto path = i.first;
        auto data = *(i.second);
        
        if (session->declared.count(path) == 0) {
          PerfCounterType type;
          type.path = path;
          if (data.description) {
            type.description = data.description;
          }
          if (data.nick) {
            type.nick = data.nick;
          }
          type.type = data.type;
          report->declare_types.push_back(std::move(type));
          session->declared.insert(path);
        }
      }
    }

    ldout(cct, 20) << by_path.size() << " counters, of which "
             << report->declare_types.size() << " new" << dendl;

    ENCODE_START(1, 1, report->packed);
    for (const auto &path : session->declared) {
      auto data = by_path.at(path);
      ::encode(static_cast<uint64_t>(data->u64.read()),
          report->packed);
      if (data->type & PERFCOUNTER_LONGRUNAVG) {
        ::encode(static_cast<uint64_t>(data->avgcount.read()),
            report->packed);
        ::encode(static_cast<uint64_t>(data->avgcount2.read()),
            report->packed);
      }
    }
    ENCODE_FINISH(report->packed);
  });

  ldout(cct, 20) << "encoded " << report->packed.length() << " bytes" << dendl;

  report->daemon_name = g_conf->name.get_id();

  session->con->send_message(report);

  if (stats_period != 0) {
    report_callback = new FunctionContext([this](int r){send_report();});
    timer.add_event_after(stats_period, report_callback);
  }

  if (pgstats_cb) {
    MPGStats *m_stats = pgstats_cb();
    session->con->send_message(m_stats);
  }
}

bool MgrClient::handle_mgr_configure(MMgrConfigure *m)
{
  assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  if (session == nullptr) {
    lderr(cct) << "dropping unexpected configure message" << dendl;
    m->put();
    return true;
  }

  ldout(cct, 4) << "stats_period=" << m->stats_period << dendl;

  bool starting = (stats_period == 0) && (m->stats_period != 0);
  stats_period = m->stats_period;
  if (starting) {
    send_report();
  }

  m->put();
  return true;
}

void MgrClient::wait_on_list(list<Cond*>& ls)
{
  assert(lock.is_locked_by_me());

  Cond cond;
  ls.push_back(&cond);
  cond.Wait(lock);
  ls.remove(&cond);
}

void MgrClient::signal_cond_list(list<Cond*>& ls)
{
  for (list<Cond*>::iterator it = ls.begin(); it != ls.end(); ++it)
    (*it)->Signal();
}

int MgrClient::start_command(const vector<string>& cmd, const bufferlist& inbl,
                  bufferlist *outbl, string *outs,
                  Context *onfinish)
{
  Mutex::Locker l(lock);

  ldout(cct, 20) << "cmd: " << cmd << dendl;

  if (session == nullptr) {
    lderr(cct) << "no session, waiting" << dendl;
    wait_on_list(waiting_for_session);
  }

  assert(map.epoch > 0);

  auto &op = command_table.start_command();
  op.cmd = cmd;
  op.inbl = inbl;
  op.outbl = outbl;
  op.outs = outs;
  op.on_finish = onfinish;

  // Leaving fsid argument null because it isn't used.
  MCommand *m = op.get_message({});
  assert(session);
  assert(session->con);
  session->con->send_message(m);

  return 0;
}

bool MgrClient::handle_command_reply(MCommandReply *m)
{
  assert(lock.is_locked_by_me());

  ldout(cct, 20) << *m << dendl;

  const auto tid = m->get_tid();
  if (!command_table.exists(tid)) {
    ldout(cct, 4) << "handle_command_reply tid " << m->get_tid()
            << " not found" << dendl;
    m->put();
    return true;
  }

  auto &op = command_table.get_command(tid);
  if (op.outbl) {
    op.outbl->claim(m->get_data());
  }

  if (op.outs) {
    *(op.outs) = m->rs;
  }

  if (op.on_finish) {
    op.on_finish->complete(m->r);
  }

  command_table.erase(tid);

  m->put();
  return true;
}

