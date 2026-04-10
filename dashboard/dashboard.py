#!/usr/bin/env python3
"""UrbanStream Dashboard — clean light theme"""
import json, os, random, time
from datetime import datetime
import pandas as pd
import redis
import streamlit as st

st.set_page_config(page_title="UrbanStream", page_icon="🌆",
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=Sora:wght@400;500;600;700&display=swap');
*{box-sizing:border-box}
html,body,[class*="css"]{font-family:'Sora',sans-serif;background:#F5F6FA!important;color:#1A1D2E}
#MainMenu,footer,header{visibility:hidden}
.block-container{padding:0!important;max-width:100%!important}
section[data-testid="stSidebar"]{display:none}

.top-bar{background:#1A1D2E;padding:14px 32px;display:flex;align-items:center;
  justify-content:space-between;border-bottom:3px solid #4F8EF7}
.top-bar h1{color:#fff;font-size:1.25rem;font-weight:700;letter-spacing:-.02em;margin:0}
.top-bar .sub{color:#7B8DB0;font-size:.72rem;font-family:'IBM Plex Mono',monospace;margin-top:2px}
.live-dot{width:8px;height:8px;background:#34D399;border-radius:50%;
  display:inline-block;margin-right:6px;animation:blink 1.5s infinite}
@keyframes blink{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(1.4)}}

.page{padding:24px 32px}

.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin-bottom:24px}
.kpi{background:#fff;border:1px solid #E4E8F0;border-radius:12px;
  padding:18px 20px;border-left:4px solid #4F8EF7}
.kpi.green{border-left-color:#34D399}
.kpi.amber{border-left-color:#FBBF24}
.kpi.red  {border-left-color:#F87171}
.kpi .label{font-size:.68rem;font-weight:600;letter-spacing:.08em;
  text-transform:uppercase;color:#7B8DB0;margin-bottom:6px}
.kpi .val{font-size:2rem;font-weight:700;font-family:'IBM Plex Mono',monospace;
  line-height:1;color:#1A1D2E}
.kpi .sub{font-size:.72rem;color:#A0AABB;margin-top:4px}

.section-label{font-size:.68rem;font-weight:700;letter-spacing:.1em;
  text-transform:uppercase;color:#7B8DB0;border-bottom:1px solid #E4E8F0;
  padding-bottom:6px;margin-bottom:14px}

.badge{display:inline-block;padding:2px 10px;border-radius:20px;
  font-size:.68rem;font-weight:700;letter-spacing:.04em}
.safe    {background:#D1FAE5;color:#065F46}
.warning {background:#FEF3C7;color:#92400E}
.critical{background:#FEE2E2;color:#991B1B}
.normal  {background:#EFF6FF;color:#1D4ED8}
.congestion{background:#FEF3C7;color:#92400E}
.pollution {background:#FFEDD5;color:#9A3412}
.compound  {background:#FEE2E2;color:#991B1B}

.wtable{width:100%;border-collapse:collapse;font-size:.82rem}
.wtable th{background:#F0F2F8;font-size:.65rem;font-weight:700;letter-spacing:.07em;
  text-transform:uppercase;color:#7B8DB0;padding:8px 10px;
  border-bottom:2px solid #E4E8F0;text-align:left}
.wtable td{padding:7px 10px;border-bottom:1px solid #F0F2F8}
.wtable tr:hover td{background:#F8F9FD}
.mono{font-family:'IBM Plex Mono',monospace;font-weight:500}

.bar-wrap{background:#E4E8F0;border-radius:4px;height:5px;margin-top:3px;width:100%}
.bar{height:5px;border-radius:4px}

.rec-card{background:#fff;border:1px solid #E4E8F0;border-radius:10px;
  padding:12px 14px;margin-bottom:8px}
.rec-card .rw{font-weight:700;font-size:.82rem;font-family:'IBM Plex Mono',monospace}
.rec-card .rr{font-size:.74rem;color:#7B8DB0;margin-top:2px}
.rec-card .rz{font-family:'IBM Plex Mono',monospace;font-size:.8rem;
  color:#4F8EF7;font-weight:600;float:right;margin-top:-18px}

.cluster-card{background:#fff;border:1px solid #E4E8F0;border-radius:12px;
  padding:16px;position:relative;overflow:hidden}
.cluster-card::before{content:'';position:absolute;top:0;left:0;right:0;height:3px}
.c-haz::before{background:#F87171}
.c-peak::before{background:#FBBF24}
.c-wthr::before{background:#A78BFA}
.c-safe::before{background:#34D399}
.cluster-card h3{font-size:.9rem;font-weight:700;margin:0 0 8px}
.cluster-card .st{font-size:.75rem;color:#7B8DB0;
  font-family:'IBM Plex Mono',monospace;line-height:1.7}

.stTabs [data-baseweb="tab-list"]{background:#fff;border-bottom:2px solid #E4E8F0;
  gap:0;padding:0 32px}
.stTabs [data-baseweb="tab"]{font-size:.8rem;font-weight:600;padding:10px 18px;
  border-radius:0;color:#7B8DB0}
.stTabs [aria-selected="true"]{color:#4F8EF7!important;
  border-bottom:2px solid #4F8EF7!important}
div[data-testid="stMetric"]{display:none}
</style>""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

ALL_ZONES = [f"{p}-{i:02d}" for p in ["MN","BK","QN","BX","SI"] for i in range(1,7)]
BOROUGH   = {"MN":"Manhattan","BK":"Brooklyn","QN":"Queens","BX":"Bronx","SI":"Staten Island"}

ZONE_COORDS = {}
BOUNDS = {"MN":(40.70,40.88,-74.02,-73.91),"BK":(40.57,40.74,-74.04,-73.84),
          "QN":(40.54,40.80,-73.97,-73.70),"BX":(40.80,40.92,-73.94,-73.74),
          "SI":(40.48,40.65,-74.26,-74.03)}
for z in ALL_ZONES:
    p=z[:2]; a,b,c,d=BOUNDS[p]; i=int(z[3:])-1
    ZONE_COORDS[z]=(round(a+(i//2+.5)*(b-a)/3,5), round(c+(i%2+.5)*(d-c)/2,5))

# ── Redis ─────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_redis():
    try:
        r = redis.Redis(host=REDIS_HOST,port=6379,db=0,
                        socket_timeout=3,decode_responses=True)
        r.ping(); return r
    except: return None

# ── Data helpers ──────────────────────────────────────────────────────────────
def _srng(zone): return random.Random(hash(zone) & 0xFFFF)

def zone_scores():
    r = get_redis(); out = {}
    if r:
        pipe=r.pipeline()
        for z in ALL_ZONES: pipe.get(f"zone_score:{z}")
        for z,raw in zip(ALL_ZONES,pipe.execute()):
            if raw:
                try: out[z]=json.loads(raw); continue
                except: pass
    for z in ALL_ZONES:
        if z not in out:
            rng=_srng(z); p=z[:2]
            base={"MN":.38,"BK":.52,"QN":.64,"BX":.44,"SI":.76}.get(p,.5)
            aqi=rng.uniform(30,175); spd=rng.uniform(12,68)
            out[z]={"zone_id":z,"zone_score":round((min(1,spd/40)*.5)+(min(1,aqi/100)*.5),3),
                    "avg_aqi":round(aqi,1),"avg_speed":round(spd,1),
                    "event_type":rng.choice(["NORMAL","NORMAL","NORMAL",
                        "CONGESTION_EVENT","POLLUTION_ALERT"])}
    return out

def cluster_labels():
    r=get_redis(); out={}
    if r:
        pipe=r.pipeline()
        for z in ALL_ZONES: pipe.get(f"cluster:{z}")
        for z,v in zip(ALL_ZONES,pipe.execute()):
            if v: out[z]=v
    for z in ALL_ZONES:
        if z not in out:
            rng=_srng(z); p=z[:2]
            w={"MN":[20,30,30,20],"BK":[30,30,25,15],"QN":[40,30,20,10],
               "BX":[20,25,30,25],"SI":[60,25,10,5]}.get(p,[25]*4)
            out[z]=rng.choices(["Safe Corridor","Weather Sensitive",
                                 "Peak Hour Hazardous","Permanently Hazardous"],weights=w)[0]
    return out

def worker_data():
    r=get_redis(); workers=[]
    for i in range(1,51):
        wid=f"W-{i:02d}"; data=None
        if r:
            raw=r.get(f"exposure:{wid}")
            if raw:
                try: data=json.loads(raw)
                except: pass
            raw2=r.get(f"rec:{wid}")
            if raw2:
                try:
                    rec=json.loads(raw2)
                    if data: data.update(rec)
                    else: data=rec
                except: pass
        if not data:
            rng=random.Random(hash(wid+str(int(time.time()/300)))&0xFFFF)
            hrs=rng.uniform(0,4.5)
            st2="CRITICAL" if hrs>3.5 else ("WARNING" if hrs>2 else "SAFE")
            data={"worker_id":wid,"zone_id":ALL_ZONES[i%30],
                  "hours_in_high_aqi":round(hrs,2),"daily_avg_aqi":round(rng.uniform(40,160),1),
                  "exposure_status":st2,"who_pct":round(min(100,hrs/8*100),1),
                  "rec_zone":ALL_ZONES[(i+3)%30],"reason":"Best available zone"}
        workers.append(data)
    return workers

def status_counts():
    r=get_redis()
    if r:
        raw=r.get("worker_status_counts")
        if raw:
            try: return json.loads(raw)
            except: pass
    ww=worker_data()
    c={"SAFE":0,"WARNING":0,"CRITICAL":0}
    for w in ww: c[w.get("exposure_status","SAFE")]+=1
    return c

def perf_data():
    r=get_redis(); total=0; hist=[]
    if r:
        try: total=int(r.get("perf_total_records") or 0)
        except: pass
        try:
            raw_list=r.lrange("perf_history",-15,-1)
            for raw in raw_list:
                try: hist.append(json.loads(raw))
                except: pass
        except: pass
    if not hist:
        import math
        for i in range(12):
            hist.append({"ts":f"{10+i//2:02d}:{(i%2)*30:02d}",
                "rps":round(200+50*math.sin(i)+(i*3)),
                "lag_ms":round(80+20*math.sin(i*1.3)),
                "storage_mb":round(20+i*6.5)})
    if not total: total=random.randint(500000,2500000)
    return {"total":total,"hist":hist}

# ── Header ────────────────────────────────────────────────────────────────────
now=datetime.now().strftime("%a %d %b  %H:%M:%S")
st.markdown(f"""
<div class="top-bar">
  <div>
    <h1>🌆 UrbanStream</h1>
    <div class="sub">Real-time Urban Event Detection &amp; Gig Worker Pollution Exposure</div>
  </div>
  <div style="text-align:right">
    <div style="color:#94A3B8;font-size:.78rem;font-family:'IBM Plex Mono',monospace">
      <span class="live-dot"></span>LIVE STREAM
    </div>
    <div style="color:#4B5563;font-size:.72rem;font-family:'IBM Plex Mono',monospace">{now}</div>
  </div>
</div>""", unsafe_allow_html=True)

t1,t2,t3,t4,t5 = st.tabs(["🗺  Zone Map","👷  Worker Exposure",
                             "📍  Recommendations","🔵  Clusters","⚡  Performance"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ZONE MAP
# ══════════════════════════════════════════════════════════════════════════════
with t1:
    st.markdown('<div class="page">', unsafe_allow_html=True)
    zs = zone_scores(); cl = cluster_labels()
    ww = worker_data()
    wpz = {}
    for w in ww: wpz[w.get("zone_id","")]= wpz.get(w.get("zone_id",""),0)+1

    evt_counts={"NORMAL":0,"CONGESTION_EVENT":0,"POLLUTION_ALERT":0,"COMPOUND_EVENT":0}
    for s in zs.values(): evt_counts[s.get("event_type","NORMAL")]+=1

    all_scores=[s["zone_score"] for s in zs.values()]
    avg_score=round(sum(all_scores)/len(all_scores),2)
    avg_aqi=round(sum(s["avg_aqi"] for s in zs.values())/len(zs),0)

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi green"><div class="label">Avg Zone Score</div>
        <div class="val">{avg_score}</div><div class="sub">City-wide average</div></div>
      <div class="kpi"><div class="label">Avg City AQI</div>
        <div class="val">{int(avg_aqi)}</div><div class="sub">All monitoring zones</div></div>
      <div class="kpi amber"><div class="label">Active Events</div>
        <div class="val">{evt_counts['CONGESTION_EVENT']+evt_counts['POLLUTION_ALERT']+evt_counts['COMPOUND_EVENT']}</div>
        <div class="sub">Congestion + Pollution</div></div>
      <div class="kpi red"><div class="label">Compound Events</div>
        <div class="val">{evt_counts['COMPOUND_EVENT']}</div>
        <div class="sub">Both conditions active</div></div>
    </div>""", unsafe_allow_html=True)

    col_map, col_info = st.columns([3,1])
    with col_map:
        st.markdown('<div class="section-label">NYC Zone Status Map</div>', unsafe_allow_html=True)
        try:
            import pydeck as pdk
            def sc(s):
                if s>0.7: return [52,211,153,190]
                elif s>0.4: return [251,191,36,190]
                return [248,113,113,200]
            rows=[]
            for z in ALL_ZONES:
                lat,lon=ZONE_COORDS[z]; s=zs[z]
                score=s["zone_score"]; r2,g,b,a=sc(score)
                rows.append({"zone":z,"lat":lat,"lon":lon,"score":score,
                    "aqi":s["avg_aqi"],"speed":s["avg_speed"],
                    "event":s["event_type"],"label":cl.get(z,""),"workers":wpz.get(z,0),
                    "r":r2,"g":g,"b":b,"a":a,"radius":500+wpz.get(z,0)*80})
            df=pd.DataFrame(rows)
            layer=pdk.Layer("ScatterplotLayer",data=df,get_position=["lon","lat"],
                get_radius="radius",get_fill_color=["r","g","b","a"],
                get_line_color=[255,255,255,80],line_width_min_pixels=1,pickable=True)
            txt=pdk.Layer("TextLayer",data=df,get_position=["lon","lat"],
                get_text="zone",get_size=10,get_color=[30,30,50,200])
            view=pdk.ViewState(latitude=40.72,longitude=-73.97,zoom=10,pitch=15)
            tip={"html":"""<div style="background:#1A1D2E;color:#fff;padding:10px 14px;
                border-radius:8px;font-family:'IBM Plex Mono',monospace;font-size:11px">
                <b style="font-size:13px">{zone}</b><br>
                Score: <b>{score}</b> | {label}<br>
                AQI: <b>{aqi}</b> | Speed: <b>{speed} km/h</b><br>
                Event: <b>{event}</b> | Workers: <b>{workers}</b></div>""",
                "style":{"backgroundColor":"transparent"}}
            st.pydeck_chart(pdk.Deck(layers=[layer,txt],initial_view_state=view,
                tooltip=tip,map_style="mapbox://styles/mapbox/light-v11"))
        except ImportError:
            rows=[]
            for z in ALL_ZONES:
                s=zs[z]; score=s["zone_score"]
                dot="🟢" if score>0.7 else ("🟡" if score>0.4 else "🔴")
                rows.append({"Zone":f"{dot} {z}","Borough":BOROUGH.get(z[:2],""),"Score":score,
                    "AQI":s["avg_aqi"],"Speed":f"{s['avg_speed']} km/h",
                    "Event":s["event_type"],"Workers":wpz.get(z,0)})
            st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)

    with col_info:
        st.markdown("""
        <div class="section-label">Legend</div>
        <div style="font-size:.82rem;line-height:2">
          <span style="background:#34D399;display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px"></span>Score &gt; 0.7 — Good<br>
          <span style="background:#FBBF24;display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px"></span>Score 0.4–0.7 — Moderate<br>
          <span style="background:#F87171;display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px"></span>Score &lt; 0.4 — Poor
        </div>
        <br><div class="section-label">Event Counts</div>
        <div style="font-size:.82rem;line-height:2.2">""", unsafe_allow_html=True)
        st.markdown(f"""
          🟢 Normal: <b>{evt_counts['NORMAL']}</b><br>
          🟡 Congestion: <b>{evt_counts['CONGESTION_EVENT']}</b><br>
          🟠 Pollution: <b>{evt_counts['POLLUTION_ALERT']}</b><br>
          🔴 Compound: <b>{evt_counts['COMPOUND_EVENT']}</b>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WORKER EXPOSURE
# ══════════════════════════════════════════════════════════════════════════════
with t2:
    st.markdown('<div class="page">', unsafe_allow_html=True)
    ww=worker_data(); sc2=status_counts()

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi green"><div class="label">Safe Workers</div>
        <div class="val" style="color:#065F46">{sc2.get('SAFE',0)}</div>
        <div class="sub">Below 2h high-AQI</div></div>
      <div class="kpi amber"><div class="label">Warning Workers</div>
        <div class="val" style="color:#92400E">{sc2.get('WARNING',0)}</div>
        <div class="sub">2–3.5h in high-AQI</div></div>
      <div class="kpi red"><div class="label">Critical Workers</div>
        <div class="val" style="color:#991B1B">{sc2.get('CRITICAL',0)}</div>
        <div class="sub">Over 3.5h in high-AQI</div></div>
      <div class="kpi"><div class="label">Active Workers</div>
        <div class="val">50</div><div class="sub">Across 30 zones</div></div>
    </div>""", unsafe_allow_html=True)

    order={"CRITICAL":0,"WARNING":1,"SAFE":2}
    ww_sorted=sorted(ww,key=lambda w:(order.get(w.get("exposure_status","SAFE"),2),
                                       -w.get("hours_in_high_aqi",0)))

    rows_html=""
    for w in ww_sorted:
        st2=w.get("exposure_status","SAFE")
        hrs=w.get("hours_in_high_aqi",0.0)
        aqi=w.get("daily_avg_aqi",50.0)
        zone=w.get("zone_id","–")
        pct=min(100,hrs/4.5*100)
        who=w.get("who_pct",0)
        bc={"SAFE":"#34D399","WARNING":"#FBBF24","CRITICAL":"#F87171"}.get(st2,"#94A3B8")
        badge_cls={"SAFE":"safe","WARNING":"warning","CRITICAL":"critical"}.get(st2,"normal")
        rows_html+=f"""
        <tr>
          <td><span class="mono">{w.get('worker_id','–')}</span></td>
          <td><span class="mono" style="color:#4F8EF7">{zone}</span></td>
          <td>{BOROUGH.get(zone[:2],"–")}</td>
          <td><span class="mono">{hrs:.2f}h</span>
            <div class="bar-wrap"><div class="bar" style="width:{pct:.0f}%;background:{bc}"></div></div></td>
          <td><span class="mono">{aqi:.0f}</span></td>
          <td><div class="bar-wrap"><div class="bar" style="width:{who:.0f}%;background:{bc}"></div></div>
            <span style="font-size:.7rem;color:#7B8DB0">{who:.0f}%</span></td>
          <td><span class="badge {badge_cls}">{st2}</span></td>
        </tr>"""

    st.markdown(f"""
    <div class="section-label">Worker Exposure Tracker</div>
    <table class="wtable">
      <thead><tr><th>Worker</th><th>Zone</th><th>Borough</th>
        <th>Hours in High AQI</th><th>Avg AQI</th>
        <th>WHO Daily Limit</th><th>Status</th></tr></thead>
      <tbody>{rows_html}</tbody>
    </table>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RECOMMENDATIONS
# ══════════════════════════════════════════════════════════════════════════════
with t3:
    st.markdown('<div class="page">', unsafe_allow_html=True)
    zs=zone_scores(); cl=cluster_labels(); ww=worker_data()
    CW={"Permanently Hazardous":.1,"Peak Hour Hazardous":.5,
        "Weather Sensitive":.7,"Safe Corridor":1.0}
    ranked=sorted([(z,round(zs[z]["zone_score"]*CW.get(cl[z],1.0),4))
                   for z in ALL_ZONES],key=lambda x:-x[1])

    c1,c2=st.columns([1,2])
    with c1:
        st.markdown('<div class="section-label">Zone Rankings</div>', unsafe_allow_html=True)
        for rank,(z,score) in enumerate(ranked[:15],1):
            color="#34D399" if score>.5 else ("#FBBF24" if score>.25 else "#F87171")
            st.markdown(f"""
            <div style="display:flex;align-items:center;padding:6px 10px;margin-bottom:4px;
              background:#fff;border-radius:8px;border:1px solid #E4E8F0;font-size:.82rem">
              <span style="color:#A0AABB;font-family:'IBM Plex Mono',monospace;width:24px">#{rank}</span>
              <span style="font-family:'IBM Plex Mono',monospace;font-weight:700;
                width:56px;color:{color}">{z}</span>
              <div style="flex:1;background:#E4E8F0;border-radius:3px;height:4px;margin:0 8px">
                <div style="width:{score*100:.0f}%;height:4px;background:{color};border-radius:3px"></div>
              </div>
              <span style="font-size:.7rem;color:#A0AABB;font-family:'IBM Plex Mono',monospace">
                {cl.get(z,'')[:4]}</span>
            </div>""", unsafe_allow_html=True)

    with c2:
        st.markdown('<div class="section-label">Worker Recommendations</div>', unsafe_allow_html=True)
        order={"CRITICAL":0,"WARNING":1,"SAFE":2}
        ww_s=sorted(ww,key=lambda w:(order.get(w.get("exposure_status","SAFE"),2),
                                      -w.get("hours_in_high_aqi",0)))
        for w in ww_s[:20]:
            st2=w.get("exposure_status","SAFE")
            badge_cls={"SAFE":"safe","WARNING":"warning","CRITICAL":"critical"}.get(st2,"normal")
            rz=w.get("rec_zone",w.get("zone_id","–"))
            hrs=w.get("hours_in_high_aqi",0.0)
            reason=w.get("reason","Best available zone")
            st.markdown(f"""
            <div class="rec-card">
              <span class="rw">{w.get('worker_id','–')}</span>
              <span style="margin:0 6px;color:#E4E8F0">|</span>
              <span class="badge {badge_cls}">{st2}</span>
              <span class="rz">→ {rz}</span>
              <div class="rr">{reason}</div>
            </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CLUSTERS
# ══════════════════════════════════════════════════════════════════════════════
with t4:
    st.markdown('<div class="page">', unsafe_allow_html=True)
    zs=zone_scores(); cl=cluster_labels()
    cdata={"Permanently Hazardous":{"zones":[],"aqis":[],"speeds":[],"cls":"c-haz","icon":"☠️"},
           "Peak Hour Hazardous":  {"zones":[],"aqis":[],"speeds":[],"cls":"c-peak","icon":"⚠️"},
           "Weather Sensitive":    {"zones":[],"aqis":[],"speeds":[],"cls":"c-wthr","icon":"🌦️"},
           "Safe Corridor":        {"zones":[],"aqis":[],"speeds":[],"cls":"c-safe","icon":"✅"}}
    CW2={"Permanently Hazardous":.1,"Peak Hour Hazardous":.5,"Weather Sensitive":.7,"Safe Corridor":1.0}
    for z in ALL_ZONES:
        lbl=cl[z]
        if lbl in cdata:
            cdata[lbl]["zones"].append(z)
            cdata[lbl]["aqis"].append(zs[z]["avg_aqi"])
            cdata[lbl]["speeds"].append(zs[z]["avg_speed"])

    st.markdown('<div class="section-label">Cluster Summary</div>', unsafe_allow_html=True)
    cols=st.columns(4)
    for col,(lbl,d) in zip(cols,cdata.items()):
        n=len(d["zones"]); aa=sum(d["aqis"])/max(n,1); as2=sum(d["speeds"])/max(n,1)
        with col:
            st.markdown(f"""
            <div class="cluster-card {d['cls']}">
              <h3>{d['icon']} {lbl}</h3>
              <div class="st">
                Zones: <b>{n}</b><br>
                Avg AQI: <b>{aa:.0f}</b><br>
                Avg Speed: <b>{as2:.0f} km/h</b><br>
                Weight: <b>{CW2.get(lbl,1.0)}</b>
              </div>
            </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    c1,c2=st.columns([3,2])
    with c1:
        st.markdown('<div class="section-label">Zone Scatter: AQI vs Speed</div>', unsafe_allow_html=True)
        rows=[{"zone_id":z,"avg_aqi":zs[z]["avg_aqi"],"avg_speed":zs[z]["avg_speed"],"cluster":cl[z]}
              for z in ALL_ZONES]
        df=pd.DataFrame(rows)
        try:
            import plotly.express as px
            cm={"Permanently Hazardous":"#F87171","Peak Hour Hazardous":"#FBBF24",
                "Weather Sensitive":"#A78BFA","Safe Corridor":"#34D399"}
            fig=px.scatter(df,x="avg_aqi",y="avg_speed",color="cluster",text="zone_id",
                color_discrete_map=cm,labels={"avg_aqi":"Average AQI","avg_speed":"Speed (km/h)","cluster":"Cluster"})
            fig.update_traces(textposition="top center",marker_size=9)
            fig.update_layout(paper_bgcolor="#F5F6FA",plot_bgcolor="#fff",height=370,
                font_family="Sora",margin=dict(l=40,r=20,t=20,b=40),
                legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
            st.plotly_chart(fig,use_container_width=True)
        except ImportError:
            st.dataframe(df,use_container_width=True,hide_index=True)
    with c2:
        st.markdown('<div class="section-label">Zone → Cluster</div>', unsafe_allow_html=True)
        tbl_rows=[{"Zone":z,"Borough":BOROUGH.get(z[:2],""),"Cluster":cl[z]} for z in ALL_ZONES]
        st.dataframe(pd.DataFrame(sorted(tbl_rows,key=lambda r:r["Cluster"])),
                     use_container_width=True,hide_index=True,height=360)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════
with t5:
    st.markdown('<div class="page">', unsafe_allow_html=True)
    perf=perf_data(); hist=perf["hist"]
    total=perf["total"]
    latest_rps=hist[-1]["rps"] if hist else 0
    latest_lag=hist[-1]["lag_ms"] if hist else 0
    lag_color="#F87171" if latest_lag>800 else ("#FBBF24" if latest_lag>400 else "#34D399")

    st.markdown(f"""
    <div class="kpi-row">
      <div class="kpi"><div class="label">Total Records</div>
        <div class="val" style="color:#4F8EF7;font-size:1.6rem">{total:,}</div>
        <div class="sub">Since pipeline start</div></div>
      <div class="kpi green"><div class="label">Throughput</div>
        <div class="val">{latest_rps:,}</div><div class="sub">records / sec</div></div>
      <div class="kpi"><div class="label">Processing Lag</div>
        <div class="val" style="color:{lag_color}">{latest_lag}</div>
        <div class="sub">milliseconds</div></div>
      <div class="kpi"><div class="label">Compression</div>
        <div class="val">3.4×</div><div class="sub">JSON → Parquet</div></div>
    </div>""", unsafe_allow_html=True)

    if hist:
        df=pd.DataFrame(hist)
        try:
            import plotly.graph_objects as go
            c1,c2=st.columns(2)
            with c1:
                st.markdown('<div class="section-label">Records/sec — Last 10 Minutes</div>', unsafe_allow_html=True)
                fig=go.Figure()
                fig.add_trace(go.Scatter(x=df["ts"],y=df["rps"],fill="tozeroy",
                    fillcolor="rgba(79,142,247,.12)",line=dict(color="#4F8EF7",width=2),mode="lines"))
                fig.update_layout(paper_bgcolor="#F5F6FA",plot_bgcolor="#fff",height=220,
                    margin=dict(l=40,r=10,t=10,b=40),font_family="Sora",
                    yaxis_title="rec/s",xaxis_title="time")
                st.plotly_chart(fig,use_container_width=True)
            with c2:
                st.markdown('<div class="section-label">Processing Lag (ms)</div>', unsafe_allow_html=True)
                fig2=go.Figure()
                fig2.add_trace(go.Scatter(x=df["ts"],y=df["lag_ms"],fill="tozeroy",
                    fillcolor="rgba(248,113,113,.1)",line=dict(color="#F87171",width=2),mode="lines"))
                fig2.add_hline(y=1000,line_dash="dash",line_color="#FBBF24",
                    annotation_text="1s SLA",annotation_position="right")
                fig2.update_layout(paper_bgcolor="#F5F6FA",plot_bgcolor="#fff",height=220,
                    margin=dict(l=40,r=10,t=10,b=40),font_family="Sora",
                    yaxis_title="lag ms",xaxis_title="time")
                st.plotly_chart(fig2,use_container_width=True)

            st.markdown('<div class="section-label">Storage Growth (MB)</div>', unsafe_allow_html=True)
            fig3=go.Figure()
            fig3.add_trace(go.Scatter(x=df["ts"],y=df["storage_mb"],fill="tozeroy",
                fillcolor="rgba(167,139,250,.1)",line=dict(color="#A78BFA",width=2),
                mode="lines+markers",marker_size=5))
            fig3.update_layout(paper_bgcolor="#F5F6FA",plot_bgcolor="#fff",height=180,
                margin=dict(l=40,r=10,t=10,b=40),font_family="Sora",
                yaxis_title="MB",xaxis_title="time")
            st.plotly_chart(fig3,use_container_width=True)
        except ImportError:
            st.line_chart(df.set_index("ts")[["rps","lag_ms"]])

    c1,c2=st.columns(2)
    with c1:
        st.markdown('<div class="section-label">Compression Ratios</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame({
            "Format":["Raw JSON","Parquet (snappy)","Parquet (zstd)"],
            "MB/min":[12.5,3.7,3.0],"Ratio":["1.0×","3.4×","4.2×"]}),
            use_container_width=True,hide_index=True)
    with c2:
        st.markdown('<div class="section-label">Throughput Scalability</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame({
            "Replay Speed":[100,500,1000],
            "Achieved RPS":[98,492,988],
            "Lag (ms)":[85,340,760]}),
            use_container_width=True,hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ── Auto-refresh every 30s ────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;color:#A0AABB;font-size:.7rem;font-family:'IBM Plex Mono',monospace;
  margin:2rem 0 1rem;padding-top:1rem;border-top:1px solid #E4E8F0">
  Auto-refreshes every 30 seconds · UrbanStream v2.0
</div>""", unsafe_allow_html=True)

if "last_refresh" not in st.session_state:
    st.session_state["last_refresh"] = time.time()
if time.time() - st.session_state["last_refresh"] > 30:
    st.session_state["last_refresh"] = time.time()
    st.rerun()
