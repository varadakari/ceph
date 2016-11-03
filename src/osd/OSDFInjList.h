#ifndef _OSD_FINJ_LIST_H
#define _OSD_FINJ_LIST_H

OSDFInj osd_finj_iot1(1,"OSD::dispatch_op_fast");
OSDFInj osd_finj_iot2(2,"OSD::dequeue_op");
OSDFInj osd_finj_iot3(3,"ReplicatedPG::do_op before find object context");
OSDFInj osd_finj_iot4(4,"ReplicatedPG::do_op after find object context");
OSDFInj osd_finj_iot5(5,"ReplicatedPG::execute_ctx");
OSDFInj osd_finj_iot6(6,"ReplicatedBackend::submit_transaction");

#endif
