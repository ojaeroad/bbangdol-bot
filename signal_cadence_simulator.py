"""저점·고점 반복 알람 축소(B안) 과거 데이터 시뮬레이터.

B안: 조건 최초 감지는 즉시 유지하고, 같은 조건이 계속 이어질 때는
UTC 기준 자연스러운 봉 경계에서만 재알림한다.
- FULL: 원 시간봉 주기 (1h -> 매 정시)
- HALF: 원 시간봉 절반 주기 (1h -> 매 30분)

성과는 저장된 LOW/HIGH 신호만 사용하며 실제 체결·수수료는 반영하지 않는다.
"""
from __future__ import annotations

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import psycopg

DATABASE_URL = os.getenv("PERFORMANCE_DATABASE_URL", "").strip()
TF_MINUTES = {"3m":3,"5m":5,"15m":15,"30m":30,"1h":60,"2h":120,"4h":240,"6h":360,"12h":720,"1d":1440,"1w":10080}
HALF_MINUTES = {"3m":3,"5m":5,"15m":5,"30m":15,"1h":30,"2h":60,"4h":120,"6h":180,"12h":360,"1d":720,"1w":5040}
GROUPS = {
    "COIN":{"SCALP":["5m","15m"],"SWING":["30m","1h"],"LONG":["4h","6h"],"LIFE":["12h","1d","1w"]},
    "KOREA":{"SWING":["30m","1h"],"LONG":["4h","6h"],"LIFE":["1d","1w"]},
    "US":{"SWING":["30m","1h"],"LONG":["4h","6h"],"LIFE":["1d","1w"]},
}
GROUP_LABEL={"SCALP":"단타","SWING":"스윙","LONG":"장기","LIFE":"인생타점"}


def _connect():
    if not DATABASE_URL:
        raise RuntimeError("PERFORMANCE_DATABASE_URL is not configured")
    return psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=8, application_name="cadence-simulator-v1")


def _market(strategy:str, exchange:str|None)->str:
    text=f"{strategy or ''} {exchange or ''}".upper()
    if strategy == "STARFLOWER": return "COIN"
    if any(x in text for x in ("KRX","KOSPI","KOSDAQ","KOREA")): return "KOREA"
    return "US"


def _group(market:str, tf:str)->str|None:
    for g,tfs in GROUPS.get(market,{}).items():
        if tf in tfs: return g
    return None


def _period_start(period_key:str)->datetime|None:
    now=datetime.now(timezone.utc)
    if period_key=="today": return now-timedelta(days=1)
    if period_key=="7d": return now-timedelta(days=7)
    if period_key=="30d": return now-timedelta(days=30)
    return None


def _load(market_filter:str, period_key:str)->list[dict[str,Any]]:
    start=_period_start(period_key)
    sql="""SELECT id,strategy,COALESCE(exchange,raw_exchange),symbol,signal_type,timeframe,
                  COALESCE(timeframe_minutes,0),signal_price,received_at
           FROM performance_signals
           WHERE signal_price IS NOT NULL AND signal_type IN ('LOW','HIGH')
             AND timeframe IS NOT NULL"""
    params=[]
    if start:
        sql += " AND received_at >= %s"; params.append(start)
    sql += " ORDER BY received_at,id"
    with _connect() as conn: rows=conn.execute(sql,params).fetchall()
    out=[]
    for r in rows:
        market=_market(r[1],r[2]); tf=str(r[5]).lower(); group=_group(market,tf)
        if market != market_filter or not group: continue
        mins=int(r[6] or TF_MINUTES.get(tf,0))
        if not mins: continue
        out.append({"id":r[0],"market":market,"exchange":r[2],"symbol":r[3],"type":r[4],"tf":tf,"mins":mins,"price":float(r[7]),"time":r[8],"group":group})
    return out


def _slot(dt:datetime, minutes:int)->int:
    return int(dt.timestamp()//(minutes*60))


def _sample(signals:list[dict[str,Any]], mode:str)->list[dict[str,Any]]:
    if mode=="ALL": return signals
    result=[]; state={}
    for s in signals:
        key=(s["symbol"],s["type"],s["tf"])
        cadence=s["mins"] if mode=="FULL" else HALF_MINUTES.get(s["tf"], max(5, s["mins"]//2))
        prev=state.get(key)
        # 2분 이상 공백이면 조건이 끊겼다가 새로 발생한 것으로 간주해 즉시 알림.
        new_episode=not prev or (s["time"]-prev["last_time"]).total_seconds()>125
        slot=_slot(s["time"],cadence)
        if new_episode or slot!=prev.get("sent_slot"):
            result.append(s)
            sent_slot=slot
        else:
            sent_slot=prev.get("sent_slot")
        state[key]={"last_time":s["time"],"sent_slot":sent_slot}
    return result


def _cycles(sampled:list[dict[str,Any]])->list[dict[str,Any]]:
    by_symbol=defaultdict(list)
    for s in sampled: by_symbol[s["symbol"]].append(s)
    cycles=[]
    for symbol, rows in by_symbol.items():
        # 포지션은 매수 그룹·최초 매수 시간봉별로 독립 구성.
        open_pos={}
        for s in rows:
            if s["type"]=="LOW":
                key=(s["group"],s["tf"])
                p=open_pos.get(key)
                if p is None:
                    open_pos[key]={"entries":[s],"last_entry":s["time"]}
                elif len(p["entries"])<3 and (s["time"]-p["last_entry"]).total_seconds()>=300:
                    p["entries"].append(s); p["last_entry"]=s["time"]
            else:
                for key,p in list(open_pos.items()):
                    group,entry_tf=key
                    if s["time"]<=p["last_entry"]: continue
                    # 현재 성과 시스템처럼 해당 시장의 스윙 이상 HIGH를 종료 후보로 허용.
                    if s["group"] not in ("SWING","LONG","LIFE") and group!="SCALP": continue
                    avg=sum(x["price"] for x in p["entries"])/len(p["entries"])
                    ret=(s["price"]-avg)/avg*100 if avg else 0.0
                    cycles.append({"symbol":symbol,"group":group,"entry_tf":entry_tf,"exit_tf":s["tf"],"return_pct":ret,"entries":len(p["entries"]),"entry_time":p["entries"][0]["time"],"exit_time":s["time"]})
                    del open_pos[key]
    return cycles


def _stats(raw_count:int, sampled_count:int, cycles:list[dict[str,Any]])->dict[str,Any]:
    vals=[c["return_pct"] for c in cycles]
    return {
        "alert_count":sampled_count,
        "alert_reduction_pct":((raw_count-sampled_count)/raw_count*100) if raw_count else 0.0,
        "completed_cycles":len(vals),
        "average_return_pct":sum(vals)/len(vals) if vals else None,
        "win_rate_pct":sum(1 for v in vals if v>0)/len(vals)*100 if vals else None,
        "best_return_pct":max(vals) if vals else None,
        "worst_return_pct":min(vals) if vals else None,
    }


def simulate_cadence(market:str, period_key:str="all")->dict[str,Any]:
    signals=_load(market,period_key)
    variants=[]
    sampled_map={}
    for code,label in (("ALL","현재 매분 방식"),("FULL","B안 · 원 시간봉 주기"),("HALF","B안 · 절반 주기")):
        sampled=_sample(signals,code); sampled_map[code]=sampled
        cycles=_cycles(sampled)
        row={"code":code,"label":label,**_stats(len(signals),len(sampled),cycles)}
        variants.append(row)
    # 시간봉별로 FULL/HALF의 알람 감소만 별도 제공
    tf_rows=[]
    for tf in sorted({s["tf"] for s in signals}, key=lambda x:TF_MINUTES.get(x,999999)):
        raw=[s for s in signals if s["tf"]==tf]
        full=_sample(raw,"FULL"); half=_sample(raw,"HALF")
        tf_rows.append({"timeframe":tf,"raw_count":len(raw),"full_count":len(full),"half_count":len(half),
                        "full_reduction_pct":((len(raw)-len(full))/len(raw)*100) if raw else 0,
                        "half_reduction_pct":((len(raw)-len(half))/len(raw)*100) if raw else 0})
    # 그룹별 성과 비교
    group_rows=[]
    for group in GROUPS.get(market,{}):
        item={"group":group,"group_label":GROUP_LABEL[group],"variants":[]}
        for code,label in (("ALL","현재"),("FULL","원 주기"),("HALF","절반 주기")):
            sig=[s for s in sampled_map[code] if s["group"]==group]
            cyc=[c for c in _cycles(sampled_map[code]) if c["group"]==group]
            item["variants"].append({"code":code,"label":label,**_stats(len([s for s in signals if s["group"]==group]),len(sig),cyc)})
        group_rows.append(item)
    return {"market":market,"period_key":period_key,"raw_signal_count":len(signals),"variants":variants,"timeframes":tf_rows,"groups":group_rows,
            "generated_at":datetime.now(timezone.utc).isoformat(),
            "note":"최초 감지는 즉시 유지하고 반복 신호만 자연 봉 경계에서 샘플링한 과거 저장 신호 기준 시뮬레이션입니다. 실제 Telegram 기본 운영값은 절반 주기(HALF)입니다."}
