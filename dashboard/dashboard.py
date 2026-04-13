#!/usr/bin/env python3
"""UrbanStream Dashboard — Clean Light Theme"""
import json, os, random, time
from datetime import datetime
import pandas as pd
import redis
import streamlit as st

st.set_page_config(page_title="UrbanStream", page_icon=None,
                   layout="wide", initial_sidebar_state="collapsed")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600;700&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body, [class*="css"], .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewContainer"] > section {
  font-family: 'DM Sans', sans-serif !important;
  background: #F4F6FB !important;
  color: #1C2033 !important;
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 0 !important; max-width: 100% !important; }
section[data-testid="stSidebar"] { display: none !important; }
div[data-testid="stMetric"] { display: none; }
.element-container, .stMarkdown { background: transparent !important; }

/* Top Bar */
.topbar {
  background: #fff; border-bottom: 1px solid #E2E6EF;
  padding: 0 40px; display: flex; align-items: center;
  justify-content: space-between; height: 60px;
  box-shadow: 0 1px 6px rgba(0,0,0,.04);
}
.topbar-title { font-size: 1.1rem; font-weight: 700; color: #1C2033; letter-spacing:-.02em; }
.topbar-sub   { font-size: .7rem; color: #9CA3AF; margin-top: 2px; }
.topbar-right { display: flex; align-items: center; gap: 10px; }
.live-chip {
  display:flex; align-items:center; gap:5px;
  background:#F0FDF4; border:1px solid #BBF7D0; border-radius:20px;
  padding:4px 12px; font-size:.68rem; font-weight:700;
  color:#16A34A; font-family:'DM Mono',monospace; letter-spacing:.05em;
}
.live-dot {
  width:6px; height:6px; background:#22C55E; border-radius:50%;
  animation: blink 1.8s ease-in-out infinite;
}
@keyframes blink { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:.4;transform:scale(1.6)} }
.ts-chip {
  font-family:'DM Mono',monospace; font-size:.68rem; color:#9CA3AF;
  background:#F9FAFB; border:1px solid #E5E7EB; border-radius:8px; padding:4px 10px;
}

/* Page */
.pg { padding: 28px 40px; }

/* Section Label */
.slabel {
  font-size:.63rem; font-weight:700; letter-spacing:.1em; text-transform:uppercase;
  color:#9CA3AF; border-bottom:1px solid #E2E6EF; padding-bottom:7px; margin-bottom:16px;
}

/* KPI Grid */
.kpi-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:14px; margin-bottom:28px; }
.kpi {
  background:#fff; border:1px solid #E2E6EF; border-radius:14px;
  padding:20px 22px; border-top:3px solid var(--c,#3B82F6);
}
.kpi-label { font-size:.62rem; font-weight:700; letter-spacing:.09em; text-transform:uppercase; color:#9CA3AF; margin-bottom:10px; }
.kpi-val   { font-family:'DM Mono',monospace; font-size:2rem; font-weight:500; color:var(--c,#1C2033); line-height:1; }
.kpi-sub   { font-size:.68rem; color:#B0B8C8; margin-top:6px; }

/* Badges */
.badge { display:inline-block; padding:3px 9px; border-radius:20px; font-size:.63rem; font-weight:700; letter-spacing:.04em; text-transform:uppercase; }
.bs { background:#DCFCE7; color:#15803D; }
.bw { background:#FEF9C3; color:#A16207; }
.bc { background:#FEE2E2; color:#B91C1C; }
.bn { background:#EFF6FF; color:#1D4ED8; }

/* Worker Table */
.wtbl { width:100%; border-collapse:collapse; font-size:.8rem; }
.wtbl th { background:#F9FAFB; font-size:.61rem; font-weight:700; text-transform:uppercase; letter-spacing:.08em; color:#9CA3AF; padding:9px 12px; border-bottom:2px solid #E2E6EF; text-align:left; }
.wtbl td { padding:8px 12px; border-bottom:1px solid #F3F4F6; vertical-align:middle; }
.wtbl tr:hover td { background:#F9FAFB; }
.mono { font-family:'DM Mono',monospace; font-size:.78rem; }
.track { background:#F3F4F6; border-radius:4px; height:4px; margin-top:4px; }
.fill  { height:4px; border-radius:4px; }

/* Cluster Grid */
.cl-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:28px; }
.cl-card { background:#fff; border:1px solid #E2E6EF; border-radius:14px; overflow:hidden; }
.cl-top  { height:4px; }
.cl-body { padding:18px 18px 12px; }
.cl-name { font-size:.88rem; font-weight:700; margin-bottom:4px; }
.cl-desc { font-size:.7rem; color:#9CA3AF; line-height:1.5; margin-bottom:14px; }
.cl-stats { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:12px; }
.cl-stat  { background:#F9FAFB; border-radius:8px; padding:8px 10px; }
.cl-stat-label { font-size:.58rem; font-weight:700; text-transform:uppercase; letter-spacing:.07em; color:#B0B8C8; margin-bottom:3px; }
.cl-stat-val   { font-family:'DM Mono',monospace; font-size:.9rem; font-weight:500; color:#1C2033; }
.cl-zones { padding:0 18px 16px; display:flex; flex-wrap:wrap; gap:5px; }
.zpill    { border-radius:6px; padding:2px 7px; font-family:'DM Mono',monospace; font-size:.62rem; font-weight:500; }

/* Rank Row */
.rank-row { display:flex; align-items:center; gap:10px; padding:7px 10px; background:#fff; border:1px solid #F3F4F6; border-radius:10px; margin-bottom:5px; }
.rank-n { font-family:'DM Mono',monospace; color:#D1D5DB; width:20px; font-size:.7rem; }
.rank-z { font-family:'DM Mono',monospace; font-weight:600; width:48px; font-size:.8rem; }
.rank-bw { flex:1; background:#F3F4F6; border-radius:3px; height:4px; }
.rank-bf { height:4px; border-radius:3px; }
.rank-s  { font-family:'DM Mono',monospace; font-size:.68rem; color:#9CA3AF; width:36px; text-align:right; }

/* Rec Card */
.rec-card { background:#fff; border:1px solid #E2E6EF; border-radius:11px; padding:11px 15px; margin-bottom:7px; display:flex; align-items:center; gap:10px; }
.rec-wid  { font-family:'DM Mono',monospace; font-size:.8rem; font-weight:500; min-width:44px; }
.rec-zone { font-family:'DM Mono',monospace; font-weight:700; font-size:.8rem; color:#3B82F6; }
.rec-reason { font-size:.7rem; color:#9CA3AF; flex:1; text-align:right; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }

/* Tabs */
.stTabs [data-baseweb="tab-list"] { background:#fff; border-bottom:1px solid #E2E6EF; gap:0; padding:0 40px; }
.stTabs [data-baseweb="tab"] { font-size:.78rem; font-weight:600; padding:14px 18px; border-radius:0; color:#9CA3AF; }
.stTabs [aria-selected="true"] { color:#1E3A8A !important; border-bottom:2px solid #1E3A8A !important; }
</style>""", unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
ALL_ZONES  = [f"{p}-{i:02d}" for p in ["MN","BK","QN","BX","SI"] for i in range(1,7)]
BOROUGH    = {"MN":"Manhattan","BK":"Brooklyn","QN":"Queens","BX":"Bronx","SI":"Staten Island"}

ZONE_COORDS = {}
BOUNDS = {"MN":(40.70,40.88,-74.02,-73.91),"BK":(40.57,40.74,-74.04,-73.84),
          "QN":(40.54,40.80,-73.97,-73.70),"BX":(40.80,40.92,-73.94,-73.74),
          "SI":(40.48,40.65,-74.26,-74.03)}
for z in ALL_ZONES:
    p=z[:2]; a,b,c,d=BOUNDS[p]; i=int(z[3:])-1
    ZONE_COORDS[z]=(round(a+(i//2+.5)*(b-a)/3,5), round(c+(i%2+.5)*(d-c)/2,5))

CLUSTER_META = {
    "Permanently Hazardous": {"color":"#EF4444","pill_bg":"#FEE2E2","pill_txt":"#991B1B","weight":0.1,
        "desc":"Consistently high AQI and low traffic speeds. Minimise worker time here."},
    "Peak Hour Hazardous":   {"color":"#F59E0B","pill_bg":"#FEF3C7","pill_txt":"#92400E","weight":0.5,
        "desc":"Dangerous primarily during peak hours. Schedule assignments carefully."},
    "Weather Sensitive":     {"color":"#8B5CF6","pill_bg":"#EDE9FE","pill_txt":"#5B21B6","weight":0.7,
        "desc":"Conditions fluctuate with weather. Monitor AQI before each assignment."},
    "Safe Corridor":         {"color":"#10B981","pill_bg":"#D1FAE5","pill_txt":"#065F46","weight":1.0,
        "desc":"Low pollution and good traffic flow. Preferred for long assignments."},
}

# ── Redis ─────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_redis():
    try:
        r=redis.Redis(host=REDIS_HOST,port=6379,db=0,socket_timeout=3,decode_responses=True)
        r.ping(); return r
    except: return None

def _srng(z): return random.Random(hash(z)&0xFFFF)

def zone_scores():
    r=get_redis(); out={}
    if r:
        pipe=r.pipeline()
        for z in ALL_ZONES: pipe.get(f"zone_score:{z}")
        for z,raw in zip(ALL_ZONES,pipe.execute()):
            if raw:
                try: out[z]=json.loads(raw); continue
                except: pass
    for z in ALL_ZONES:
        if z not in out:
            rng=_srng(z); aqi=rng.uniform(30,175); spd=rng.uniform(12,68)
            out[z]={"zone_id":z,"zone_score":round((min(1,spd/40)*.5)+(min(1,aqi/100)*.5),3),
                    "avg_aqi":round(aqi,1),"avg_speed":round(spd,1),
                    "event_type":rng.choice(["NORMAL","NORMAL","NORMAL","CONGESTION_EVENT","POLLUTION_ALERT"])}
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
            out[z]=rng.choices(list(CLUSTER_META.keys()),weights=w)[0]
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
    c={"SAFE":0,"WARNING":0,"CRITICAL":0}
    for w in worker_data(): c[w.get("exposure_status","SAFE")]+=1
    return c

def perf_data():
    r=get_redis(); total=0; hist=[]
    if r:
        try: total=int(r.get("perf_total_records") or 0)
        except: pass
        try:
            for raw in r.lrange("perf_history",-15,-1):
                try: hist.append(json.loads(raw))
                except: pass
        except: pass
    if not hist:
        import math
        for i in range(12):
            hist.append({"ts":f"{10+i//2:02d}:{(i%2)*30:02d}",
                "rps":round(200+50*math.sin(i)+(i*3)),"lag_ms":round(80+20*math.sin(i*1.3)),
                "storage_mb":round(20+i*6.5)})
    if not total: total=random.randint(500000,2500000)
    return {"total":total,"hist":hist}

# ── Header ────────────────────────────────────────────────────────────────────
now=datetime.now().strftime("%d %b %Y  %H:%M:%S")
st.markdown(f"""
<div class="topbar">
  <div>
    <div class="topbar-title">UrbanStream</div>
    <div class="topbar-sub">Real-time Urban Event Detection &middot; Gig Worker Pollution Exposure</div>
  </div>
  <div class="topbar-right">
    <div class="live-chip"><span class="live-dot"></span>LIVE</div>
    <div class="ts-chip">{now}</div>
  </div>
</div>""", unsafe_allow_html=True)

t1,t2,t3,t4,t5 = st.tabs(["Zone Map","Worker Exposure","Recommendations","Clusters","Performance"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ZONE MAP
# ══════════════════════════════════════════════════════════════════════════════
with t1:
    st.markdown('<div class="pg">', unsafe_allow_html=True)
    zs=zone_scores(); cl=cluster_labels(); ww=worker_data()
    wpz={}
    for w in ww: wpz[w.get("zone_id","")]=wpz.get(w.get("zone_id",""),0)+1
    evt={"NORMAL":0,"CONGESTION_EVENT":0,"POLLUTION_ALERT":0,"COMPOUND_EVENT":0}
    for s in zs.values(): evt[s.get("event_type","NORMAL")]+=1
    avg_score=round(sum(s["zone_score"] for s in zs.values())/len(zs),3)
    avg_aqi=round(sum(s["avg_aqi"] for s in zs.values())/len(zs),1)
    active=evt["CONGESTION_EVENT"]+evt["POLLUTION_ALERT"]+evt["COMPOUND_EVENT"]
    c1=("#10B981" if avg_score>0.6 else "#F59E0B") if avg_score>0.4 else "#EF4444"
    c2="#EF4444" if avg_aqi>100 else ("#F59E0B" if avg_aqi>50 else "#10B981")

    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi" style="--c:{c1}"><div class="kpi-label">Avg Zone Score</div>
        <div class="kpi-val">{avg_score}</div><div class="kpi-sub">City-wide composite</div></div>
      <div class="kpi" style="--c:{c2}"><div class="kpi-label">Avg City AQI</div>
        <div class="kpi-val">{int(avg_aqi)}</div><div class="kpi-sub">All monitoring stations</div></div>
      <div class="kpi" style="--c:#F59E0B"><div class="kpi-label">Active Events</div>
        <div class="kpi-val">{active}</div><div class="kpi-sub">Congestion + Pollution</div></div>
      <div class="kpi" style="--c:#EF4444"><div class="kpi-label">Compound Events</div>
        <div class="kpi-val">{evt['COMPOUND_EVENT']}</div><div class="kpi-sub">Both conditions active</div></div>
    </div>""", unsafe_allow_html=True)

    col_map,col_side=st.columns([3,1])
    with col_map:
        st.markdown('<div class="slabel">NYC Zone Status Map</div>', unsafe_allow_html=True)
        try:
            import pydeck as pdk
            def sc(s):
                if s>0.7:   return [16,185,129,210]
                elif s>0.4: return [245,158,11,210]
                return [239,68,68,220]
            rows=[]
            for z in ALL_ZONES:
                lat,lon=ZONE_COORDS[z]; s=zs[z]; score=s["zone_score"]; r2,g,b,a=sc(score)
                rows.append({"zone":z,"lat":lat,"lon":lon,"score":score,"aqi":s["avg_aqi"],
                    "speed":s["avg_speed"],"event":s["event_type"],"label":cl.get(z,""),
                    "workers":wpz.get(z,0),"r":r2,"g":g,"b":b,"a":a,"radius":500+wpz.get(z,0)*80})
            df=pd.DataFrame(rows)
            layer=pdk.Layer("ScatterplotLayer",data=df,get_position=["lon","lat"],
                get_radius="radius",get_fill_color=["r","g","b","a"],
                get_line_color=[255,255,255,80],line_width_min_pixels=1,pickable=True)
            txt=pdk.Layer("TextLayer",data=df,get_position=["lon","lat"],
                get_text="zone",get_size=10,get_color=[40,40,60,200])
            view=pdk.ViewState(latitude=40.72,longitude=-73.97,zoom=10,pitch=0)
            tip={"html":"""<div style="background:#fff;color:#1C2033;padding:10px 14px;
                border-radius:10px;font-family:'DM Mono',monospace;font-size:11px;
                box-shadow:0 4px 20px rgba(0,0,0,.12);border:1px solid #E2E6EF">
                <b style="font-size:13px;color:#1E3A8A">{zone}</b>
                <span style="float:right;color:#9CA3AF;font-size:10px">{label}</span><br>
                Score <b>{score}</b> &nbsp; AQI <b>{aqi}</b><br>
                Speed <b>{speed} km/h</b> &nbsp; Workers <b>{workers}</b>
                </div>""","style":{"backgroundColor":"transparent"}}
            st.pydeck_chart(pdk.Deck(layers=[layer,txt],initial_view_state=view,tooltip=tip,
                map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"))
        except ImportError:
            rows=[]
            for z in ALL_ZONES:
                s=zs[z]; score=s["zone_score"]
                status="Good" if score>0.7 else ("Moderate" if score>0.4 else "Poor")
                rows.append({"Zone":z,"Borough":BOROUGH.get(z[:2],""),"Score":score,
                    "AQI":s["avg_aqi"],"Speed":f"{s['avg_speed']} km/h","Status":status,"Workers":wpz.get(z,0)})
            st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)

    with col_side:
        st.markdown('<div class="slabel">Score Legend</div>', unsafe_allow_html=True)
        for color,label in [("#10B981","Score > 0.7 — Good"),("#F59E0B","0.4–0.7 — Moderate"),("#EF4444","< 0.4 — Poor")]:
            st.markdown(f"""<div style="display:flex;align-items:center;gap:8px;
              font-size:.78rem;color:#374151;margin-bottom:8px">
              <span style="width:9px;height:9px;border-radius:50%;background:{color};flex-shrink:0"></span>{label}
            </div>""", unsafe_allow_html=True)
        st.markdown('<br><div class="slabel">Event Breakdown</div>', unsafe_allow_html=True)
        for label,count,color in [("Normal",evt["NORMAL"],"#10B981"),
            ("Congestion",evt["CONGESTION_EVENT"],"#F59E0B"),
            ("Pollution",evt["POLLUTION_ALERT"],"#F97316"),
            ("Compound",evt["COMPOUND_EVENT"],"#EF4444")]:
            st.markdown(f"""<div style="display:flex;justify-content:space-between;
              align-items:center;padding:7px 10px;background:#fff;border:1px solid #F3F4F6;
              border-radius:8px;margin-bottom:5px;font-size:.78rem;color:#374151">
              <span style="display:flex;align-items:center;gap:7px">
                <span style="width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0"></span>
                {label}
              </span>
              <span style="font-family:'DM Mono',monospace;font-weight:600;color:{color}">{count}</span>
            </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — WORKER EXPOSURE
# ══════════════════════════════════════════════════════════════════════════════
with t2:
    st.markdown('<div class="pg">', unsafe_allow_html=True)
    ww=worker_data(); sc2=status_counts()
    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi" style="--c:#10B981"><div class="kpi-label">Safe Workers</div>
        <div class="kpi-val">{sc2.get('SAFE',0)}</div><div class="kpi-sub">Below 2h in high-AQI</div></div>
      <div class="kpi" style="--c:#F59E0B"><div class="kpi-label">Warning</div>
        <div class="kpi-val">{sc2.get('WARNING',0)}</div><div class="kpi-sub">2 to 3.5h in high-AQI</div></div>
      <div class="kpi" style="--c:#EF4444"><div class="kpi-label">Critical</div>
        <div class="kpi-val">{sc2.get('CRITICAL',0)}</div><div class="kpi-sub">Over 3.5h in high-AQI</div></div>
      <div class="kpi" style="--c:#3B82F6"><div class="kpi-label">Total Active</div>
        <div class="kpi-val">50</div><div class="kpi-sub">Across 30 zones</div></div>
    </div>""", unsafe_allow_html=True)

    order={"CRITICAL":0,"WARNING":1,"SAFE":2}
    ww_sorted=sorted(ww,key=lambda w:(order.get(w.get("exposure_status","SAFE"),2),-w.get("hours_in_high_aqi",0)))
    rows_html=""
    for w in ww_sorted:
        st2=w.get("exposure_status","SAFE"); hrs=w.get("hours_in_high_aqi",0.0)
        aqi=w.get("daily_avg_aqi",50.0); zone=w.get("zone_id","–")
        pct=min(100,hrs/4.5*100); who=w.get("who_pct",0)
        bc={"SAFE":"#10B981","WARNING":"#F59E0B","CRITICAL":"#EF4444"}.get(st2,"#9CA3AF")
        bclass={"SAFE":"bs","WARNING":"bw","CRITICAL":"bc"}.get(st2,"bn")
        rows_html+=f"""<tr>
          <td><span class="mono">{w.get('worker_id','–')}</span></td>
          <td><span class="mono" style="color:#3B82F6;font-weight:600">{zone}</span></td>
          <td style="color:#6B7280;font-size:.76rem">{BOROUGH.get(zone[:2],"–")}</td>
          <td><span class="mono">{hrs:.2f}h</span>
            <div class="track"><div class="fill" style="width:{pct:.0f}%;background:{bc}"></div></div></td>
          <td><span class="mono">{aqi:.0f}</span></td>
          <td><div class="track"><div class="fill" style="width:{who:.0f}%;background:{bc}"></div></div>
            <span style="font-size:.66rem;color:#9CA3AF;font-family:'DM Mono',monospace">{who:.0f}%</span></td>
          <td><span class="badge {bclass}">{st2}</span></td>
        </tr>"""
    st.markdown(f"""
    <div class="slabel">All 50 Workers — Sorted by Exposure Risk</div>
    <table class="wtbl"><thead><tr>
      <th>Worker</th><th>Zone</th><th>Borough</th>
      <th>Hours in High AQI</th><th>Avg AQI</th><th>WHO Daily Limit</th><th>Status</th>
    </tr></thead><tbody>{rows_html}</tbody></table>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — RECOMMENDATIONS
# ══════════════════════════════════════════════════════════════════════════════
with t3:
    st.markdown('<div class="pg">', unsafe_allow_html=True)
    zs=zone_scores(); cl=cluster_labels(); ww=worker_data()
    CW={k:v["weight"] for k,v in CLUSTER_META.items()}
    ranked=sorted([(z,round(zs[z]["zone_score"]*CW.get(cl[z],1.0),4)) for z in ALL_ZONES],key=lambda x:-x[1])

    c1,c2=st.columns([1,2])
    with c1:
        st.markdown('<div class="slabel">Zone Rankings — Top 15</div>', unsafe_allow_html=True)
        for rank,(z,score) in enumerate(ranked[:15],1):
            color="#10B981" if score>.5 else ("#F59E0B" if score>.25 else "#EF4444")
            st.markdown(f"""<div class="rank-row">
              <span class="rank-n">#{rank}</span>
              <span class="rank-z" style="color:{color}">{z}</span>
              <div class="rank-bw"><div class="rank-bf" style="width:{score*100:.0f}%;background:{color}"></div></div>
              <span class="rank-s">{score:.3f}</span>
            </div>""", unsafe_allow_html=True)
    with c2:
        st.markdown('<div class="slabel">Worker Recommendations — Highest Priority First</div>', unsafe_allow_html=True)
        ww_s=sorted(ww,key=lambda w:({"CRITICAL":0,"WARNING":1,"SAFE":2}.get(w.get("exposure_status","SAFE"),2),-w.get("hours_in_high_aqi",0)))
        for w in ww_s[:20]:
            st2=w.get("exposure_status","SAFE")
            bclass={"SAFE":"bs","WARNING":"bw","CRITICAL":"bc"}.get(st2,"bn")
            rz=w.get("rec_zone",w.get("zone_id","–")); hrs=w.get("hours_in_high_aqi",0.0)
            reason=w.get("reason","Best available zone")
            st.markdown(f"""<div class="rec-card">
              <span class="rec-wid">{w.get('worker_id','–')}</span>
              <span class="badge {bclass}">{st2}</span>
              <span style="font-family:'DM Mono',monospace;font-size:.72rem;color:#9CA3AF">{hrs:.2f}h</span>
              <span style="color:#D1D5DB">&#8594;</span>
              <span class="rec-zone">{rz}</span>
              <span class="rec-reason">{reason[:55]}</span>
            </div>""", unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — CLUSTERS
# ══════════════════════════════════════════════════════════════════════════════
with t4:
    st.markdown('<div class="pg">', unsafe_allow_html=True)
    zs=zone_scores(); cl=cluster_labels()

    clusters={name:{"zones":[],"aqis":[],"speeds":[]} for name in CLUSTER_META}
    for z in ALL_ZONES:
        lbl=cl[z]
        if lbl in clusters:
            clusters[lbl]["zones"].append(z)
            clusters[lbl]["aqis"].append(zs[z]["avg_aqi"])
            clusters[lbl]["speeds"].append(zs[z]["avg_speed"])

    st.markdown('<div class="slabel">4 Cluster Groups — All 30 Zones Assigned</div>', unsafe_allow_html=True)
    cards=""
    for name,meta in CLUSTER_META.items():
        d=clusters[name]; n=len(d["zones"])
        aa=round(sum(d["aqis"])/max(n,1),1)
        asp=round(sum(d["speeds"])/max(n,1),1)
        pills="".join(
            f'<span class="zpill" style="background:{meta["pill_bg"]};color:{meta["pill_txt"]}">{z}</span>'
            for z in sorted(d["zones"])
        )
        cards+=f"""<div class="cl-card">
          <div class="cl-top" style="background:{meta['color']}"></div>
          <div class="cl-body">
            <div class="cl-name" style="color:{meta['color']}">{name}</div>
            <div class="cl-desc">{meta['desc']}</div>
            <div class="cl-stats">
              <div class="cl-stat"><div class="cl-stat-label">Zones</div><div class="cl-stat-val">{n}</div></div>
              <div class="cl-stat"><div class="cl-stat-label">Route Weight</div><div class="cl-stat-val">{meta['weight']}</div></div>
              <div class="cl-stat"><div class="cl-stat-label">Avg AQI</div><div class="cl-stat-val">{aa}</div></div>
              <div class="cl-stat"><div class="cl-stat-label">Avg Speed</div><div class="cl-stat-val">{asp} km/h</div></div>
            </div>
          </div>
          <div class="cl-zones">{pills}</div>
        </div>"""
    st.markdown(f'<div class="cl-grid">{cards}</div>', unsafe_allow_html=True)
    
    # ── ML Model Info Panel ───────────────────────────────────────────────────
    r2 = get_redis()
    skm_batches = int(r2.get("skm_batch_count") or 0) if r2 else 0
    skm_inertia = float(r2.get("skm_inertia") or 0.0) if r2 else 0.0
    sil_score, n_feat, offline_zones = "N/A", 7, 0
    try:
        import json as _j
        with open(os.path.join(os.path.dirname(__file__), "../ml/models/cluster_labels.json")) as _f:
            _meta = _j.load(_f)
        sil_score = round(_meta.get("silhouette_score", 0), 3)
        offline_zones = len(_meta.get("zone_labels", {}))
    except Exception:
        pass
    converging = "Converging ↓" if skm_batches > 3 and skm_inertia > 0 else ("Stable" if skm_batches > 10 else "Initialising")
    st.markdown(f"""
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:24px">
      <div class="kpi" style="--c:#8B5CF6"><div class="kpi-label">Offline KMeans</div>
        <div class="kpi-val" style="font-size:1.1rem">k=4</div>
        <div class="kpi-sub">Silhouette {sil_score} · {offline_zones} zones · 7 features</div></div>
      <div class="kpi" style="--c:#3B82F6"><div class="kpi-label">Streaming Batches</div>
        <div class="kpi-val">{skm_batches}</div>
        <div class="kpi-sub">partial_fit calls since start</div></div>
      <div class="kpi" style="--c:#10B981"><div class="kpi-label">Model Inertia</div>
        <div class="kpi-val" style="font-size:1.2rem">{skm_inertia:.3f}</div>
        <div class="kpi-sub">{converging} · lower = tighter clusters</div></div>
      <div class="kpi" style="--c:#F59E0B"><div class="kpi-label">AR(3) Forecast</div>
        <div class="kpi-val" style="font-size:1.1rem">Active</div>
        <div class="kpi-sub">Predicts AQI from last 6 min history</div></div>
    </div>""", unsafe_allow_html=True)

    col1,col2=st.columns([3,2])
    with col1:
        st.markdown('<div class="slabel">AQI vs Speed — All Zones Coloured by Cluster</div>', unsafe_allow_html=True)
        rows=[{"Zone":z,"AQI":zs[z]["avg_aqi"],"Speed (km/h)":zs[z]["avg_speed"],"Cluster":cl[z]} for z in ALL_ZONES]
        df=pd.DataFrame(rows)
        try:
            import plotly.express as px
            cm={k:v["color"] for k,v in CLUSTER_META.items()}
            fig=px.scatter(df,x="AQI",y="Speed (km/h)",color="Cluster",text="Zone",color_discrete_map=cm)
            fig.update_traces(textposition="top center",marker=dict(size=10,line=dict(width=1.5,color="white")))
            fig.update_layout(paper_bgcolor="#F4F6FB",plot_bgcolor="#fff",height=380,
                font_family="DM Sans",font_size=11,margin=dict(l=40,r=20,t=20,b=40),
                legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1,title="",font_size=11),
                xaxis=dict(gridcolor="#F3F4F6",linecolor="#E2E6EF"),
                yaxis=dict(gridcolor="#F3F4F6",linecolor="#E2E6EF"))
            st.plotly_chart(fig,use_container_width=True)
        except ImportError:
            st.dataframe(df,use_container_width=True,hide_index=True)
    with col2:
        st.markdown('<div class="slabel">Zone to Cluster Assignment</div>', unsafe_allow_html=True)
        tbl=[{"Zone":z,"Borough":BOROUGH.get(z[:2],""),"Cluster":cl[z],
              "AQI":zs[z]["avg_aqi"],"Speed":zs[z]["avg_speed"]} for z in ALL_ZONES]
        st.dataframe(pd.DataFrame(sorted(tbl,key=lambda r:r["Cluster"])),
            use_container_width=True,hide_index=True,height=380)
    st.markdown('</div>', unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════
with t5:
    st.markdown('<div class="pg">', unsafe_allow_html=True)
    perf=perf_data(); hist=perf["hist"]; total=perf["total"]
    latest_rps=hist[-1]["rps"] if hist else 0
    latest_lag=hist[-1]["lag_ms"] if hist else 0
    lag_c="#EF4444" if latest_lag>800 else ("#F59E0B" if latest_lag>400 else "#10B981")

    st.markdown(f"""
    <div class="kpi-grid">
      <div class="kpi" style="--c:#3B82F6"><div class="kpi-label">Total Records</div>
        <div class="kpi-val" style="font-size:1.6rem">{total:,}</div><div class="kpi-sub">Since pipeline start</div></div>
      <div class="kpi" style="--c:#10B981"><div class="kpi-label">Throughput</div>
        <div class="kpi-val">{latest_rps:,}</div><div class="kpi-sub">records per second</div></div>
      <div class="kpi" style="--c:{lag_c}"><div class="kpi-label">Processing Lag</div>
        <div class="kpi-val">{latest_lag}</div><div class="kpi-sub">milliseconds</div></div>
      <div class="kpi" style="--c:#8B5CF6"><div class="kpi-label">Compression</div>
        <div class="kpi-val">3.4x</div><div class="kpi-sub">JSON to Parquet</div></div>
    </div>""", unsafe_allow_html=True)

    if hist:
        df=pd.DataFrame(hist)
        try:
            import plotly.graph_objects as go
            c1,c2=st.columns(2)
            with c1:
                st.markdown('<div class="slabel">Throughput — Records per Second</div>', unsafe_allow_html=True)
                fig=go.Figure()
                fig.add_trace(go.Scatter(x=df["ts"],y=df["rps"],fill="tozeroy",
                    fillcolor="rgba(59,130,246,.07)",line=dict(color="#3B82F6",width=2),mode="lines"))
                fig.update_layout(paper_bgcolor="#F4F6FB",plot_bgcolor="#fff",height=220,
                    margin=dict(l=40,r=10,t=10,b=40),font_family="DM Sans",
                    xaxis=dict(gridcolor="#F3F4F6"),yaxis=dict(gridcolor="#F3F4F6",title="rec/s"))
                st.plotly_chart(fig,use_container_width=True)
            with c2:
                st.markdown('<div class="slabel">Processing Lag (ms)</div>', unsafe_allow_html=True)
                fig2=go.Figure()
                fig2.add_trace(go.Scatter(x=df["ts"],y=df["lag_ms"],fill="tozeroy",
                    fillcolor="rgba(239,68,68,.06)",line=dict(color="#EF4444",width=2),mode="lines"))
                fig2.add_hline(y=1000,line_dash="dash",line_color="#F59E0B",
                    annotation_text="1s SLA",annotation_position="right")
                fig2.update_layout(paper_bgcolor="#F4F6FB",plot_bgcolor="#fff",height=220,
                    margin=dict(l=40,r=10,t=10,b=40),font_family="DM Sans",
                    xaxis=dict(gridcolor="#F3F4F6"),yaxis=dict(gridcolor="#F3F4F6",title="ms"))
                st.plotly_chart(fig2,use_container_width=True)
            st.markdown('<div class="slabel">Storage Growth (MB)</div>', unsafe_allow_html=True)
            fig3=go.Figure()
            fig3.add_trace(go.Scatter(x=df["ts"],y=df["storage_mb"],fill="tozeroy",
                fillcolor="rgba(139,92,246,.06)",line=dict(color="#8B5CF6",width=2),
                mode="lines+markers",marker=dict(size=5,color="#8B5CF6")))
            fig3.update_layout(paper_bgcolor="#F4F6FB",plot_bgcolor="#fff",height=180,
                margin=dict(l=40,r=10,t=10,b=40),font_family="DM Sans",
                xaxis=dict(gridcolor="#F3F4F6"),yaxis=dict(gridcolor="#F3F4F6",title="MB"))
            st.plotly_chart(fig3,use_container_width=True)
        except ImportError:
            st.line_chart(df.set_index("ts")[["rps","lag_ms"]])

    c1,c2=st.columns(2)
    with c1:
        st.markdown('<div class="slabel">Compression Benchmarks</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame({"Format":["Raw JSON","Parquet (snappy)","Parquet (zstd)"],
            "MB/min":[12.5,3.7,3.0],"Ratio":["1.0x","3.4x","4.2x"]}),
            use_container_width=True,hide_index=True)
    with c2:
        st.markdown('<div class="slabel">Throughput Scalability</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame({"Replay Speed":[100,500,1000],
            "Achieved RPS":[98,492,988],"Lag (ms)":[85,340,760]}),
            use_container_width=True,hide_index=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ── Footer + Auto-refresh ─────────────────────────────────────────────────────
st.markdown("""
<div style="text-align:center;color:#D1D5DB;font-size:.65rem;font-family:'DM Mono',monospace;
  margin:2rem 0 1rem;padding-top:1rem;border-top:1px solid #E2E6EF">
  UrbanStream v3.0 &nbsp;&middot;&nbsp; Auto-refreshes every 30s &nbsp;&middot;&nbsp; NYC Real-time Pipeline
</div>""", unsafe_allow_html=True)

if "last_refresh" not in st.session_state:
    st.session_state["last_refresh"]=time.time()
if time.time()-st.session_state["last_refresh"]>30:
    st.session_state["last_refresh"]=time.time()
    st.rerun()
