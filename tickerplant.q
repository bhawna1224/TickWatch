/ ---- schema ----
trade:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$())

/ ---- subscriber registry ----
.u.w:(1#`)!enlist ()
.u.w[`trade]:()

/ ---- subscription handler (dedupes on handle so re-subscribing doesn't create doubles) ----
.u.sub:{[t;s]
  if[not t in key .u.w; '"unknown table: ",string t];
  .u.w[t]:(.u.w[t] where not .z.w=first each .u.w[t]),enlist (.z.w;s);
  (t; $[t~`trade; trade; ()])
  }

/ ---- publish to all subscribers of a table ----
.u.pub:{[t;data]
  w:.u.w[t];
  if[not count w; :()];
  {[t;data;sub]
    h:sub 0; s:sub 1;
    if[not h in key `.z.W; :()];
    neg[h] (`upd;t;$[s~`;data;select from data where sym in s])
    }[t;data] each w;
  }

/ ---- crash-recovery log, with automatic date rollover ----
.u.curDate:.z.d
.u.L:hsym `$"tplogs/",string .u.curDate
if[not type key .u.L; (.u.L) set ()]
.u.l:hopen .u.L
.u.j:0

/ rolls to a fresh log file named for the new date - called lazily, on the next tick after midnight
.u.roll:{
  hclose .u.l;
  .u.curDate::.z.d;
  .u.L::hsym `$"tplogs/",string .u.curDate;
  if[not type key .u.L;(.u.L) set ()];
  .u.l::hopen .u.L;
  .u.j::0;
  -1 "tickerplant: rolled log to ",string .u.L;
  }

.u.checkroll:{if[.z.d<>.u.curDate;.u.roll[]]}

/ ---- main entry point: feed handler / test client calls this ----
.u.upd:{[t;data]
  .u.checkroll[];
  data:$[0>type data; enlist data; data];
  .u.l enlist (`upd;t;data);
  .u.j+:1;
  .u.pub[t;data];
  }

/ ---- cleanup dead handles on disconnect ----
.z.pc:{[h]
  .u.w:{[h;x] x where not h=first each x}[h] each .u.w;
  }

-1 "tickerplant listening on port 5010, log file: ",string .u.L;
