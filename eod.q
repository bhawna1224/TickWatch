/ ---- EOD process ----
/ Load into the RDB process:  \l eod.q
/ Trigger manually for now:   .eod.run[]
/ Later this becomes triggered by the tickerplant at day-end instead of by hand.
/ NOTE: every assignment to a variable that already exists OUTSIDE this function
/ (trade) must use :: (global assign), not : (local assign). A single : anywhere
/ in a function body makes q treat that name as LOCAL for the WHOLE function,
/ even on lines above the assignment - this bit us once already, be careful
/ if you extend this.
/ NOTE: .Q.dpft's last arg is BOTH the source global to read AND the on-disk
/ table name it writes. We must pass `trade itself (not a scratch name like `t)
/ or the HDB ends up with a table named after the scratch variable instead.

.eod.run:{
  if[not `trade in key `.;-1"EOD: no trade table in memory, nothing to do";:()];
  if[0=count trade;-1"EOD: trade table is empty, nothing to save";:()];
  full::trade;
  ds:distinct `date$full`time;
  -1 "EOD: found ",string[count ds]," distinct date(s) in trade: ",", " sv string ds;
  {[d]
    trade::select from full where (`date$time)=d;
    .Q.dpft[`:hdb;d;`sym;`trade];
    -1 "EOD: wrote ",string[count trade]," rows to hdb/",string[d],"/trade";
    } each ds;
  delete full from `.;
  trade::0#trade;
  -1 "EOD: RDB trade table cleared for next session.";
  }
