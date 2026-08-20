/ ---- schema (must match the tickerplant's) ----
trade:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$())

/ ---- called by the tickerplant on every publish, and by log replay ----
upd:{[t;data]
  data:$[0>type data; enlist data; data];
  t insert data;
  }

/ ---- connection state ----
.rdb.h:0Ni          / current handle to the tickerplant; 0Ni means "not connected"

/ ---- (re)connect: open handle, subscribe, replay today's log to rebuild state exactly ----
/ Used for both the first startup AND recovering after the tickerplant restarts -
/ a full re-replay means we rebuild from the log rather than trust whatever
/ partial state was in memory before the disconnect, avoiding gaps or duplicates.
.rdb.connect:{
  h2:@[hopen;`::5010;{-1 "RDB: connect failed - ",x; 0Ni}];
  if[null h2; :(::)];
  .rdb.h::h2;
  r:@[h2;(`.u.sub;`trade;`);{-1 "RDB: subscribe failed - ",x; `FAIL}];
  if[r~`FAIL; .rdb.h::0Ni; :(::)];
  trade::0#trade;
  logfile:hsym `$"tplogs/",string .z.d;
  if[type key logfile; -11!logfile];
  -1 "RDB (re)connected. rows after replay: ",string count trade;
  }

/ ---- periodic check: is the tickerplant connection alive? if not, reconnect ----
/ A dead handle isn't announced proactively - we find out by trying a trivial
/ synchronous call and seeing if it errors.
.rdb.checkConn:{
  if[null .rdb.h; .rdb.connect[]; :(::)];
  ok:@[{.rdb.h "1"};(::);{0b}];
  if[not (1~ok);
    -1 "RDB: tickerplant connection lost, reconnecting...";
    .rdb.h::0Ni;
    .rdb.connect[];
    ];
  }

/ ---- auto-load EOD logic (defines .eod.checkSchedule, sets up no timer of its own) ----
\l eod.q

/ ---- single combined timer: connection health check AND EOD schedule check ----
.z.ts:{ .rdb.checkConn[]; .eod.checkSchedule[]; }
\t 5000

/ ---- initial connection ----
.rdb.connect[]
