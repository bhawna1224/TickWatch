/ ---- schema (must match the tickerplant's) ----
trade:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$())

/ ---- called by the tickerplant on every publish, and by log replay ----
upd:{[t;data]
  data:$[0>type data; enlist data; data];
  t insert data;
  }

/ ---- connect and subscribe FIRST (before replay) ----
h:hopen `::5010
resp:h (`.u.sub;`trade;`)     / registers us; tickerplant starts queueing future ticks to us now

/ ---- replay today's log to catch up on anything published before we subscribed ----
logfile:hsym `$"tplogs/",string .z.d
if[type key logfile; -11!logfile]

-1 "RDB up. rows loaded from replay: ",string count trade;