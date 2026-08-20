/ ---- anomaly detector ----
/ subscribes to the tickerplant's `trade` table, maintains a rolling window
/ per symbol, flags ticks whose DETRENDED PRICE RETURN or LOG-SIZE falls in
/ the extreme tail (top/bottom .a.lowPct/.a.hiPct) of that symbol's recent
/ history.
/ Prior versions ranked the RAW return directly. That still over-flagged
/ during ordinary short-term trends: a sustained drift (even a mild one)
/ means almost every new tick sets a fresh local extreme simply by
/ continuing the trend, not because anything unusual happened.
/ Fix: subtract a SHORT moving average of recent returns (the local
/ momentum) from the current return before ranking. This "detrended
/ residual" (an innovation, in time-series terms) is near zero during a
/ steady trend - only a genuine acceleration, reversal, or break from the
/ established short-term trend produces a large residual, which is what we
/ actually want to catch.
/ Size is unaffected by this - it doesn't have the same drift/trend problem,
/ so it's still scored as LOG(size) percentile rank directly.
/ .a.replaying is set true while rebuilding state from a log replay (on
/ startup or after a reconnect) - the rolling windows still get rebuilt
/ correctly, but we don't re-flag/re-print/re-insert historical anomalies
/ that were already handled before a disconnect.

.a.N:100            / rolling window size per symbol
.a.minN:20          / minimum history before we start flagging
.a.shortN:5         / short window used to estimate local momentum, subtracted out before ranking price
.a.lowPct:0.005f    / flag if in the bottom 0.5% of recent history
.a.hiPct:0.995f     / flag if in the top 0.5% of recent history

.a.lastPrice:()!()  / sym -> most recent price seen (to compute the next return)
.a.rWin:()!()       / sym -> float list of recent RAW returns (used only to compute local short-term momentum)
.a.drWin:()!()      / sym -> float list of recent DETRENDED return residuals (what we actually rank)
.a.lvWin:()!()      / sym -> float list of recent LOG(size) values
.a.replaying:0b     / true while rebuilding state from a log replay - suppresses flagging side-effects

trade:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$())
anomalies:([] time:`timestamp$(); sym:`symbol$(); price:`float$(); size:`float$(); priceRank:`float$(); sizeRank:`float$(); reason:())
allScores:([] time:`timestamp$(); sym:`symbol$(); priceRank:`float$(); sizeRank:`float$())

/ percentile rank of v within historical window x, in [0,1]. 0=lowest seen, 1=highest seen.
.a.pctrank:{[x;v]
  n:count x;
  $[n=0;0n;((sum x<v)+0.5*sum x=v)%n]
  }

.a.proc:{[row]
  s:row`sym; p:row`price; v:row`size; tm:row`time;
  lastP:$[s in key .a.lastPrice;.a.lastPrice s;0n];
  rh:$[s in key .a.rWin;.a.rWin s;`float$()];
  drh:$[s in key .a.drWin;.a.drWin s;`float$()];
  lvh:$[s in key .a.lvWin;.a.lvWin s;`float$()];
  r:$[null lastP;0n;(p-lastP)%lastP];
  lv:$[v>0;log v;0n];
  shortMA:$[count[rh]>=.a.shortN;avg neg[.a.shortN]#rh;0f];
  dr:$[null r;0n;r-shortMA];
  pr:0n; vr:0n; reasons:`symbol$();
  if[(not null dr) and count[drh]>=.a.minN; pr:.a.pctrank[drh;dr]];
  if[(v>0) and count[lvh]>=.a.minN; vr:.a.pctrank[lvh;lv]];
  if[not .a.replaying;
    `allScores insert (tm;s;pr;vr);
    if[(not null pr) and ((pr<=.a.lowPct) or (pr>=.a.hiPct)); reasons,:`price];
    if[(not null vr) and ((vr<=.a.lowPct) or (vr>=.a.hiPct)); reasons,:`size];
    if[count reasons;
      `anomalies insert (tm;s;p;v;pr;vr;reasons);
      -1 "ANOMALY ",string[s]," price=",string[p]," size=",string[v]," priceRank=",(string pr)," sizeRank=",(string vr)," reason=",", " sv string reasons;
      ];
    ];
  if[not null r; .a.rWin[s]::neg[.a.N] sublist rh,r];
  if[not null dr; .a.drWin[s]::neg[.a.N] sublist drh,dr];
  .a.lastPrice[s]::p;
  if[v>0; .a.lvWin[s]::neg[.a.N] sublist lvh,lv];
  }

/ t insert data lets kdb+ unpack a raw positional tuple into a real, named
/ row against our `trade` schema (same trick the RDB uses) - insert returns
/ the index/indices of the row(s) just added, so we know exactly what's new.
upd:{[t;data]
  idx:t insert data;
  .a.proc each trade idx;
  }

/ ---- connection state ----
.a.h:0Ni

/ ---- (re)connect: open handle, subscribe, replay today's log to rebuild the
/ rolling windows exactly - quietly (no re-flagging historical anomalies) ----
.a.connect:{
  h2:@[hopen;`::5010;{-1 "ANOM: connect failed - ",x; 0Ni}];
  if[null h2; :(::)];
  .a.h::h2;
  r:@[h2;(`.u.sub;`trade;`);{-1 "ANOM: subscribe failed - ",x; `FAIL}];
  if[r~`FAIL; .a.h::0Ni; :(::)];
  trade::0#trade;
  .a.lastPrice::()!();
  .a.rWin::()!();
  .a.drWin::()!();
  .a.lvWin::()!();
  .a.replaying::1b;
  logfile:hsym `$"tplogs/",string .z.d;
  if[type key logfile; -11!logfile];
  .a.replaying::0b;
  -1 "ANOM (re)connected. rows after replay: ",string[count trade],", windows rebuilt quietly.";
  }

/ ---- periodic check: is the tickerplant connection alive? if not, reconnect ----
.a.checkConn:{
  if[null .a.h; .a.connect[]; :(::)];
  ok:@[{.a.h "1"};(::);{0b}];
  if[not (1~ok);
    -1 "ANOM: tickerplant connection lost, reconnecting...";
    .a.h::0Ni;
    .a.connect[];
    ];
  }

.z.ts:{.a.checkConn[]}
\t 5000

/ ---- initial connection ----
.a.connect[]
