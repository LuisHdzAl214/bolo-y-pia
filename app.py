import streamlit as st
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError
import pandas as pd
import os
import json
from datetime import datetime, timedelta, time as dtime
import threading
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from apscheduler.schedulers.background import BackgroundScheduler
import plotly.graph_objects as go
#cd "C:\Users\herna\OneDrive\Documentos\LUIS\Escuela\TEC\Semestre 6\Toño Ortiz\boloypia"
#python -m streamlit run app.py
# =====================================================
# CONFIGURACIÓN — Streamlit Cloud usa st.secrets
# En local puedes usar .streamlit/secrets.toml
# =====================================================
def get_secret(key, default=""):
    try:
        return st.secrets[key]
    except Exception:
        return os.getenv(key, default)

MONGO_URI              = get_secret("MONGO_URI")
GMAIL_USER             = get_secret("GMAIL_USER")
GMAIL_APP_PASSWORD     = get_secret("GMAIL_APP_PASSWORD")
PUBLIC_ACCESS_PASSWORD = get_secret("PUBLIC_ACCESS_PASSWORD", "familia123")
DEVELOPER_PASSWORD     = get_secret("DEVELOPER_PASSWORD", "dev_secreto")
APP_URL                = get_secret("APP_URL", "")

# ─── Familia — agrega el email de cada quien ──────────────────────
# Formato: ("Nombre", "email@ejemplo.com")
# Deja el email en blanco ("") si alguien no quiere recibir correos.
FAMILIA = [
    ("Luis",         "hernandezpla214@gmail.com"),
]
NOMBRES_FAMILIA = [f[0] for f in FAMILIA]
TODOS_EMAILS    = [f[1] for f in FAMILIA if f[1]]  # filtra vacíos

# =====================================================
# CONEXIÓN MONGODB — singleton en session_state
# =====================================================
@st.cache_resource
def get_mongo_db():
    """
    Crea una única conexión a MongoDB Atlas y la reutiliza.
    Configura índices la primera vez.
    """
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db     = client["boloypía"]

    # Índices para búsquedas rápidas por fecha
    db.tareas.create_index([("fecha", ASCENDING), ("tarea", ASCENDING)])
    db.historial.create_index([("timestamp", DESCENDING)])
    db.email_logs.create_index([("timestamp", DESCENDING)])
    db.inventario.create_index([("producto", ASCENDING)], unique=True)
    db.turno_semanal.create_index([("semana", ASCENDING)], unique=True)

    return db

def db():
    return get_mongo_db()

# =====================================================
# CONFIGURACIÓN PÁGINA
# =====================================================
st.set_page_config(page_title="Bolo y Pía 🐶", page_icon="🐾", layout="centered")

# =====================================================
# CSS DARK MODE (idéntico al original)
# =====================================================
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
    :root {
        --bg-base:    #0d0f14;
        --bg-surface: #141720;
        --bg-card:    #1a1e2b;
        --bg-hover:   #20263a;
        --border:     #2a3048;
        --border-glow:#3d4f8a;
        --accent:     #f97316;
        --accent2:    #fb923c;
        --accent-glow:rgba(249,115,22,0.25);
        --green:      #22d3a5;
        --red:        #f43f5e;
        --yellow:     #fbbf24;
        --text-main:  #e8ecf4;
        --text-muted: #8892a4;
        --text-dim:   #525e78;
    }
    .stApp, [data-testid="stAppViewContainer"] {
        background: var(--bg-base) !important;
        background-image:
            radial-gradient(ellipse 80% 50% at 50% -10%, rgba(249,115,22,0.08) 0%, transparent 60%),
            repeating-linear-gradient(0deg, transparent, transparent 39px, rgba(255,255,255,0.015) 40px),
            repeating-linear-gradient(90deg, transparent, transparent 39px, rgba(255,255,255,0.015) 40px) !important;
    }
    html, body, [class*="css"] { font-family: 'Syne', sans-serif !important; color: var(--text-main) !important; }
    h1 { font-family: 'Syne', sans-serif !important; font-weight: 800 !important; font-size: 2.2rem !important; color: var(--text-main) !important; letter-spacing: -1px !important; line-height: 1.1 !important; }
    h2, h3, h4 { font-family: 'Syne', sans-serif !important; font-weight: 700 !important; color: var(--text-main) !important; letter-spacing: -0.3px !important; }
    p, span, label, div { color: var(--text-main) !important; }
    .stCaption, [data-testid="stCaptionContainer"] p { color: var(--text-muted) !important; font-family: 'JetBrains Mono', monospace !important; font-size: 0.75rem !important; }
    [data-testid="stVerticalBlock"] { background: transparent !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 3px !important; background: var(--bg-surface) !important; border-radius: 12px !important; padding: 5px !important; border: 1px solid var(--border) !important; }
    .stTabs [data-baseweb="tab"] { border-radius: 9px !important; padding: 7px 14px !important; font-weight: 600 !important; font-size: 0.78rem !important; color: var(--text-muted) !important; border: none !important; background: transparent !important; transition: all 0.18s ease !important; font-family: 'Syne', sans-serif !important; letter-spacing: 0.3px !important; }
    .stTabs [data-baseweb="tab"]:hover { color: var(--text-main) !important; background: var(--bg-hover) !important; }
    .stTabs [aria-selected="true"] { background: var(--accent) !important; color: #fff !important; box-shadow: 0 0 16px var(--accent-glow), 0 2px 6px rgba(0,0,0,0.4) !important; }
    .stTabs [data-baseweb="tab-panel"] { padding-top: 18px !important; }
    .stButton > button { background: var(--bg-card) !important; border: 1px solid var(--border) !important; border-radius: 8px !important; color: var(--text-main) !important; font-family: 'Syne', sans-serif !important; font-weight: 600 !important; font-size: 0.82rem !important; transition: all 0.15s ease !important; }
    .stButton > button:hover { background: var(--bg-hover) !important; border-color: var(--border-glow) !important; transform: translateY(-1px) !important; }
    .stButton > button[kind="primary"] { background: var(--accent) !important; border: none !important; color: #fff !important; box-shadow: 0 0 20px var(--accent-glow) !important; }
    .stButton > button[kind="primary"]:hover { background: var(--accent2) !important; box-shadow: 0 0 30px var(--accent-glow) !important; }
    .stButton > button:disabled { background: var(--bg-surface) !important; color: var(--text-dim) !important; border-color: var(--border) !important; opacity: 0.5 !important; }
    [data-testid="stVerticalBlockBorderWrapper"] { border-radius: 12px !important; border: 1px solid var(--border) !important; background: var(--bg-card) !important; transition: border-color 0.2s, box-shadow 0.2s !important; }
    [data-testid="stVerticalBlockBorderWrapper"]:hover { border-color: var(--border-glow) !important; box-shadow: 0 0 20px rgba(61,79,138,0.3) !important; }
    .stTextInput > div > div > input, .stSelectbox > div > div, .stNumberInput > div > div > input { background: var(--bg-surface) !important; border: 1px solid var(--border) !important; border-radius: 8px !important; color: var(--text-main) !important; font-family: 'Syne', sans-serif !important; }
    .stTextInput > div > div > input:focus { border-color: var(--accent) !important; box-shadow: 0 0 0 2px var(--accent-glow) !important; }
    .stTextInput label, .stSelectbox label, .stNumberInput label { color: var(--text-muted) !important; font-size: 0.78rem !important; font-weight: 600 !important; letter-spacing: 0.5px !important; text-transform: uppercase !important; }
    .stProgress > div > div { background: var(--bg-surface) !important; border-radius: 6px !important; border: 1px solid var(--border) !important; }
    .stProgress > div > div > div { background: linear-gradient(90deg, var(--accent), var(--accent2)) !important; border-radius: 6px !important; box-shadow: 0 0 10px var(--accent-glow) !important; }
    [data-testid="stMetric"] { background: var(--bg-card) !important; border-radius: 10px !important; border: 1px solid var(--border) !important; padding: 14px !important; }
    [data-testid="stMetric"] label { color: var(--text-muted) !important; font-size: 0.72rem !important; text-transform: uppercase !important; letter-spacing: 0.8px !important; }
    [data-testid="stMetricValue"] { color: var(--text-main) !important; font-size: 1.7rem !important; font-weight: 800 !important; }
    [data-testid="stAlert"] { border-radius: 10px !important; border-left-width: 3px !important; }
    .stSuccess > div { background: rgba(34,211,165,0.08) !important; border-color: var(--green) !important; color: var(--green) !important; }
    .stSuccess > div p { color: var(--green) !important; }
    .stWarning > div { background: rgba(251,191,36,0.08) !important; border-color: var(--yellow) !important; }
    .stWarning > div p { color: var(--yellow) !important; }
    .stError > div { background: rgba(244,63,94,0.08) !important; border-color: var(--red) !important; }
    .stError > div p { color: var(--red) !important; }
    .stInfo > div { background: rgba(61,79,138,0.15) !important; border-color: var(--border-glow) !important; }
    [data-testid="stSidebar"] { background: var(--bg-surface) !important; border-right: 1px solid var(--border) !important; }
    [data-testid="stSidebar"] * { color: var(--text-main) !important; }
    [data-testid="stExpander"] { background: var(--bg-surface) !important; border: 1px solid var(--border) !important; border-radius: 10px !important; }
    .stDataFrame { border-radius: 10px !important; overflow: hidden !important; }
    .stDataFrame thead th { background: var(--bg-hover) !important; color: var(--text-muted) !important; font-size: 0.75rem !important; text-transform: uppercase !important; letter-spacing: 0.5px !important; }
    .stDataFrame tbody tr { background: var(--bg-card) !important; }
    .stDataFrame tbody tr:hover { background: var(--bg-hover) !important; }
    hr { border-color: var(--border) !important; opacity: 0.6 !important; }
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--bg-base); }
    ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 4px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--border-glow); }
    .hero-header { text-align: center; padding: 16px 0 8px 0; }
    .hero-paws { font-size: 2.4rem; animation: bounce 2.5s ease-in-out infinite; display: inline-block; }
    @keyframes bounce { 0%, 100% { transform: translateY(0) rotate(0deg); } 40% { transform: translateY(-8px) rotate(-5deg); } 60% { transform: translateY(-5px) rotate(3deg); } }
    .responsable-badge { display: inline-block; background: linear-gradient(135deg, var(--accent), var(--accent2)); color: #fff; border-radius: 20px; padding: 3px 13px; font-size: 0.78rem; font-weight: 700; letter-spacing: 0.3px; box-shadow: 0 0 12px var(--accent-glow); }
    .nombre-banner { background: rgba(249,115,22,0.1); border: 1px solid rgba(249,115,22,0.3); border-radius: 10px; padding: 10px 16px; margin-bottom: 8px; }
    [data-baseweb="popover"] { background: var(--bg-card) !important; border: 1px solid var(--border) !important; }
    [role="option"] { background: var(--bg-card) !important; color: var(--text-main) !important; }
    [role="option"]:hover { background: var(--bg-hover) !important; }
    .stRadio label { color: var(--text-main) !important; }
    .stDownloadButton > button { background: var(--bg-surface) !important; border: 1px solid var(--border) !important; color: var(--text-main) !important; font-family: 'Syne', sans-serif !important; border-radius: 8px !important; }
    .stDownloadButton > button:hover { border-color: var(--accent) !important; }
    @media (max-width: 768px) {
        [data-testid="stSidebar"] { display: none !important; }
        [data-testid="stSidebarCollapsedControl"] { display: none !important; }
        .stMainBlockContainer, [data-testid="stAppViewBlockContainer"] { padding-left: 12px !important; padding-right: 12px !important; max-width: 100% !important; }
        h1 { font-size: 1.6rem !important; }
        .stTabs [data-baseweb="tab-list"] { overflow-x: auto !important; flex-wrap: nowrap !important; -webkit-overflow-scrolling: touch !important; }
        .stTabs [data-baseweb="tab"] { white-space: nowrap !important; padding: 8px 12px !important; font-size: 0.82rem !important; flex-shrink: 0 !important; }
        .stButton > button { min-height: 48px !important; width: 100% !important; }
        .stTextInput > div > div > input, .stNumberInput > div > div > input { min-height: 48px !important; font-size: 1rem !important; }
        [data-testid="stHorizontalBlock"] { flex-direction: column !important; }
        [data-testid="stHorizontalBlock"] > [data-testid="stVerticalBlock"] { width: 100% !important; min-width: 100% !important; }
    }
</style>
""", unsafe_allow_html=True)

# =====================================================
# DEFINICIÓN DE TAREAS (sin cambios)
# =====================================================
TAREAS_RECREATIVAS = [
    {"key": "paseo",  "label": "🦮 Paseo del día",     "desc": "¿Ya sacaron a pasear a Bolo y Pía?",
     "alerta": "17:30", "duracion": True},
    {"key": "salud",  "label": "❤️ Revisión de salud", "desc": "¿Todo bien con Bolo y Pía hoy?",
     "alerta": "20:00", "nota": True},
]
TAREAS_MANTENIMIENTO = [
    {"key": "agua",    "label": "💧 ¿Bolo y Pía tienen agua limpia?"},
    {"key": "espacio", "label": "🧹 ¿El espacio de Bolo y Pía está limpio?", "nota": True},
]
COMIDAS = [
    {"key": "desayuno", "label": "🌅 Desayuno", "hora_inicio": "07:00", "hora_fin": "11:00",
     "reminder": "10:30", "alerta": "11:01"},
    {"key": "comida",   "label": "☀️ Comida",   "hora_inicio": "13:00", "hora_fin": "15:00",
     "reminder": "14:30", "alerta": "15:01"},
    {"key": "cena",     "label": "🌙 Cena",     "hora_inicio": "19:00", "hora_fin": "21:00",
     "reminder": "20:30", "alerta": "21:01"},
]
INVENTARIO_CATEGORIAS = {
    "bolo":  {"label": "🐕 Comida Bolo",    "productos": ["Croquetas Bolo", "Latas Bolo"]},
    "pia":   {"label": "🐩 Comida Pía",     "productos": ["Croquetas Pía", "Latas Pía"]},
    "otros": {"label": "🎁 Snacks y Otros", "productos": ["Snacks", "Latas Mix", "Otro"]},
}

# =====================================================
# HELPERS DE FECHA
# =====================================================
def semana_actual():
    hoy   = datetime.now().date()
    lunes = hoy - timedelta(days=hoy.weekday())
    return lunes.strftime("%Y-%m-%d")

def _parse_time(t_str: str) -> dtime:
    h, m = map(int, t_str.split(":"))
    return dtime(hour=h, minute=m)

def ventana_activa(comida: dict) -> bool:
    ahora    = datetime.now().time()
    t_inicio = _parse_time(comida["hora_inicio"])
    t_fin    = _parse_time(comida["hora_fin"])
    if t_inicio <= t_fin:
        return t_inicio <= ahora <= t_fin
    return ahora >= t_inicio or ahora <= t_fin

# =====================================================
# CAPA DE DATOS — MongoDB
# Todas las funciones sustituyen al sqlite3 original.
# Se mantienen los mismos nombres para que el resto
# del código no cambie.
# =====================================================

# ── Tareas ───────────────────────────────────────────
def get_tareas_hoy() -> pd.DataFrame:
    hoy  = datetime.now().strftime("%Y-%m-%d")
    docs = list(db().tareas.find({"fecha": hoy}, {"_id": 0}))
    return pd.DataFrame(docs) if docs else pd.DataFrame(
        columns=["fecha","tarea","completada","quien","timestamp","notas"])

def marcar_tarea(tarea_key: str, completada: bool, quien: str, notas: str = ""):
    hoy   = datetime.now().strftime("%Y-%m-%d")
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db().tareas.update_one(
        {"fecha": hoy, "tarea": tarea_key},
        {"$set": {"completada": completada, "quien": quien,
                  "timestamp": ahora, "notas": notas}},
        upsert=True,
    )
    registrar_historial(tarea_key, quien, "marcada" if completada else "desmarcada", notas)

def registrar_historial(tarea: str, quien: str, accion: str, notas: str = ""):
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db().historial.insert_one({
        "timestamp": ahora, "tarea": tarea, "quien": quien,
        "accion": accion, "notas": notas,
    })

def get_estado_tarea(df_hoy: pd.DataFrame, tarea_key: str):
    if df_hoy.empty:
        return None
    row = df_hoy[df_hoy["tarea"] == tarea_key]
    return None if row.empty else row.iloc[0]

def comida_dada_hoy(comida_key: str) -> bool:
    hoy = datetime.now().strftime("%Y-%m-%d")
    doc = db().tareas.find_one({"fecha": hoy, "tarea": comida_key}, {"completada": 1})
    return bool(doc and doc.get("completada"))

def tarea_sin_registrar_n_dias(tarea_key: str, n_dias: int = 5) -> bool:
    fecha_limite = (datetime.now() - timedelta(days=n_dias)).strftime("%Y-%m-%d")
    count = db().tareas.count_documents({
        "tarea": tarea_key, "completada": True,
        "fecha": {"$gte": fecha_limite},
    })
    return count == 0

# ── Email logs ───────────────────────────────────────
def log_email(tipo: str, asunto: str, destinatarios: list, enviado: bool, error: str = ""):
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db().email_logs.insert_one({
        "timestamp":     ahora,
        "tipo":          tipo,
        "asunto":        asunto,
        "destinatarios": destinatarios,
        "enviado":       enviado,
        "error":         error,
    })

# ── Inventario ───────────────────────────────────────
def get_inventario() -> pd.DataFrame:
    docs = list(db().inventario.find({}, {"_id": 0}).sort("producto", ASCENDING))
    return pd.DataFrame(docs) if docs else pd.DataFrame(
        columns=["producto","cantidad","unidad","minimo","categoria","actualizado"])

def upsert_inventario(producto: str, cantidad: float, unidad: str,
                      minimo: float, categoria: str):
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db().inventario.update_one(
        {"producto": producto},
        {"$set": {"cantidad": cantidad, "unidad": unidad,
                  "minimo": minimo, "categoria": categoria,
                  "actualizado": ahora}},
        upsert=True,
    )

def check_inventario_bajo() -> list:
    docs = list(db().inventario.find({}, {"_id": 0}))
    if not docs:
        return []
    return [d for d in docs if d.get("cantidad", 0) <= d.get("minimo", 0)]

# ── Turno semanal ────────────────────────────────────
def get_turno_semana(semana: str = None):
    if semana is None:
        semana = semana_actual()
    doc = db().turno_semanal.find_one({"semana": semana}, {"_id": 0})
    if doc:
        return (doc["responsable"], doc["email"])
    return None

def set_turno_semana(semana: str, responsable: str, email: str):
    db().turno_semanal.update_one(
        {"semana": semana},
        {"$set": {"responsable": responsable, "email": email}},
        upsert=True,
    )

# =====================================================
# EMAIL — Gmail SMTP con contraseña de aplicación
# =====================================================
def _build_html(asunto: str, cuerpo: str) -> str:
    """Convierte texto plano en un HTML bonito para el email."""
    lineas = cuerpo.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    lineas_html = "".join(f"<p style='margin:4px 0'>{l if l.strip() else '&nbsp;'}</p>"
                          for l in lineas.splitlines())
    return f"""
    <html><body style="font-family:Arial,sans-serif;background:#f4f4f4;padding:20px">
      <div style="max-width:520px;margin:auto;background:#fff;border-radius:12px;
                  box-shadow:0 2px 8px rgba(0,0,0,.1);overflow:hidden">
        <div style="background:#f97316;padding:18px 24px">
          <h2 style="color:#fff;margin:0">🐾 Bolo y Pía</h2>
        </div>
        <div style="padding:20px 24px;color:#222;line-height:1.6">
          {lineas_html}
        </div>
        <div style="background:#f9f9f9;padding:10px 24px;font-size:12px;color:#888">
          App familiar · <a href="{APP_URL}" style="color:#f97316">{APP_URL}</a>
        </div>
      </div>
    </body></html>"""

def send_email(asunto: str, cuerpo: str, tipo: str = "info",
               solo_responsable: bool = False):
    """
    Envía un correo a la familia (o solo al responsable) usando Gmail SMTP.
    Requiere GMAIL_USER y GMAIL_APP_PASSWORD en los secrets.
    """
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        log_email(tipo, asunto, [], False, "Gmail no configurado en secrets")
        return

    if solo_responsable:
        turno       = get_turno_semana()
        destinatarios = [turno[1]] if (turno and turno[1]) else TODOS_EMAILS
    else:
        destinatarios = TODOS_EMAILS

    if not destinatarios:
        log_email(tipo, asunto, [], False, "Sin destinatarios — agrega emails en FAMILIA")
        return

    html = _build_html(asunto, cuerpo)
    errores = []
    for dest in destinatarios:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = f"🐾 {asunto}"
            msg["From"]    = f"Bolo y Pía <{GMAIL_USER}>"
            msg["To"]      = dest
            msg.attach(MIMEText(cuerpo, "plain", "utf-8"))
            msg.attach(MIMEText(html,   "html",  "utf-8"))
            with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
                server.sendmail(GMAIL_USER, dest, msg.as_string())
        except Exception as e:
            errores.append(f"{dest}: {e}")

    if errores:
        log_email(tipo, asunto, destinatarios, False, " | ".join(errores))
    else:
        log_email(tipo, asunto, destinatarios, True)

# =====================================================
# SCHEDULER (APScheduler — corre en el proceso Streamlit)
# Nota: Streamlit Cloud puede "dormir" la app tras
# inactividad. Para producción 24/7 se recomienda
# complementar con un ping externo (ver README).
# =====================================================
_scheduler_lock = threading.Lock()

def get_responsable_turno() -> str:
    turno = get_turno_semana()
    return turno[0] if turno else "la familia"

def make_reminder_job(comida):
    def job():
        if not comida_dada_hoy(comida["key"]):
            resp   = get_responsable_turno()
            asunto = f"⏰ Recordatorio {comida['label']} — Bolo y Pía"
            cuerpo = (f"Faltan 30 min para cerrar {comida['label']} (hasta {comida['hora_fin']}).\n"
                      f"¡Nadie ha registrado que ya comieron!\n\n"
                      f"Responsable esta semana: {resp}\n"
                      f"App: {APP_URL}")
            send_email(asunto, cuerpo, tipo=f"reminder_{comida['key']}", solo_responsable=False)
    return job

def make_alerta_job(comida):
    def job():
        if not comida_dada_hoy(comida["key"]):
            resp   = get_responsable_turno()
            asunto = f"🚨 ALERTA {comida['label']} sin registrar — Bolo y Pía"
            cuerpo = (f"{comida['label']} ya cerró ({comida['hora_fin']}) y nadie registró.\n\n"
                      f"Responsable: {resp}\nApp: {APP_URL}")
            send_email(asunto, cuerpo, tipo=f"alerta_{comida['key']}", solo_responsable=False)
        else:
            asunto = f"✅ {comida['label']} completada — Bolo y Pía"
            cuerpo = f"Bolo y Pía ya tuvieron su {comida['label']} hoy. ❤️\nApp: {APP_URL}"
            send_email(asunto, cuerpo, tipo=f"confirmacion_{comida['key']}", solo_responsable=False)
    return job

def make_alerta_recreativa_job(tarea):
    def job():
        hoy = datetime.now().strftime("%Y-%m-%d")
        doc = db().tareas.find_one({"fecha": hoy, "tarea": tarea["key"]}, {"completada": 1})
        if not (doc and doc.get("completada")):
            resp  = get_responsable_turno()
            sin_5 = tarea_sin_registrar_n_dias(tarea["key"], 5)
            if sin_5:
                asunto = f"🚨 ALERTA 5+ días — {tarea['label']} sin registrar"
                cuerpo = (f"{tarea['label']} lleva 5 o más días sin registrarse.\n\n"
                          f"Responsable actual: {resp}\n"
                          f"¡Por favor revisen el cuidado de Bolo y Pía!\nApp: {APP_URL}")
                send_email(asunto, cuerpo, tipo=f"alerta_5dias_{tarea['key']}", solo_responsable=False)
            else:
                asunto = f"🔔 Recordatorio — {tarea['label']}"
                cuerpo = (f"{tarea['label']} no se ha registrado hoy.\n\n"
                          f"Responsable: {resp}\nApp: {APP_URL}")
                send_email(asunto, cuerpo, tipo=f"alerta_{tarea['key']}", solo_responsable=True)
    return job

def make_mantenimiento_alerta_job(tarea):
    def job():
        hoy = datetime.now().strftime("%Y-%m-%d")
        doc = db().tareas.find_one({"fecha": hoy, "tarea": tarea["key"]}, {"completada": 1})
        if not (doc and doc.get("completada")):
            resp  = get_responsable_turno()
            sin_5 = tarea_sin_registrar_n_dias(tarea["key"], 5)
            if sin_5:
                asunto = f"🚨 ALERTA 5+ días — {tarea['label']} sin registrar"
                cuerpo = (f"{tarea['label']} lleva 5 o más días sin registrarse.\n\n"
                          f"Responsable actual: {resp}\nApp: {APP_URL}")
                send_email(asunto, cuerpo, tipo=f"alerta_5dias_{tarea['key']}", solo_responsable=False)
            else:
                asunto = f"🔔 Recordatorio — {tarea['label']}"
                cuerpo = (f"{tarea['label']} no se ha registrado hoy.\n\n"
                          f"Responsable: {resp}\nApp: {APP_URL}")
                send_email(asunto, cuerpo, tipo=f"alerta_{tarea['key']}", solo_responsable=True)
    return job

def make_inventario_check_job():
    def job():
        bajos = check_inventario_bajo()
        if bajos:
            lista  = "\n".join([f"• {p['producto']}: {p['cantidad']} {p['unidad']} (mín: {p['minimo']})"
                                 for p in bajos])
            asunto = "📦 Alerta de inventario — Bolo y Pía"
            cuerpo = f"Los siguientes productos están por acabarse:\n\n{lista}\n\n¡Hay que reponerlos! 🛒\nApp: {APP_URL}"
            send_email(asunto, cuerpo, tipo="inventario_bajo", solo_responsable=False)
    return job

def init_scheduler():
    scheduler = BackgroundScheduler(timezone="America/Mexico_City")
    for comida in COMIDAS:
        r_h, r_m = map(int, comida["reminder"].split(":"))
        a_h, a_m = map(int, comida["alerta"].split(":"))
        scheduler.add_job(make_reminder_job(comida), "cron", hour=r_h, minute=r_m,
                          id=f"reminder_{comida['key']}", replace_existing=True)
        scheduler.add_job(make_alerta_job(comida),   "cron", hour=a_h, minute=a_m,
                          id=f"alerta_{comida['key']}", replace_existing=True)
    for tarea in TAREAS_RECREATIVAS:
        a_h, a_m = map(int, tarea["alerta"].split(":"))
        scheduler.add_job(make_alerta_recreativa_job(tarea), "cron", hour=a_h, minute=a_m,
                          id=f"alerta_rec_{tarea['key']}", replace_existing=True)
    for tarea in TAREAS_MANTENIMIENTO:
        scheduler.add_job(make_mantenimiento_alerta_job(tarea), "cron", hour=21, minute=30,
                          id=f"alerta_mant_{tarea['key']}", replace_existing=True)
    scheduler.add_job(make_inventario_check_job(), "cron", hour=9, minute=0,
                      id="check_inventario", replace_existing=True)
    scheduler.start()
    return scheduler

if "scheduler_started" not in st.session_state:
    with _scheduler_lock:
        if "scheduler_started" not in st.session_state:
            try:
                st.session_state.scheduler = init_scheduler()
                st.session_state.scheduler_started = True
            except Exception as e:
                st.session_state.scheduler_started = False

# =====================================================
# AUTENTICACIÓN — password simple (sin ngrok)
# =====================================================
def is_developer():
    return st.session_state.get("is_developer", False)

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.title("🔒 Acceso Familiar — Bolo y Pía")
    st.markdown("Ingresa la contraseña para acceder 🐾")
    pwd = st.text_input("Contraseña:", type="password")
    if st.button("Ingresar", type="primary"):
        if pwd == PUBLIC_ACCESS_PASSWORD:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("❌ Contraseña incorrecta")
    st.stop()

# =====================================================
# SIDEBAR
# =====================================================
with st.sidebar:
    st.header("🐾 Bolo y Pía")
    turno = get_turno_semana()
    if turno:
        st.success(f"📅 Responsable esta semana:\n**{turno[0]}**")
    else:
        st.warning("📅 Sin responsable esta semana")

    st.divider()
    st.markdown("**👤 ¿Quién eres?**")
    nombre_input = st.text_input(
        "Tu nombre:",
        placeholder="Ej: Mamá, Luis, Papá, Ines...",
        value=st.session_state.get("nombre_usuario", ""),
        key="nombre_sidebar",
        label_visibility="collapsed",
    )
    if nombre_input:
        st.session_state.nombre_usuario = nombre_input
    if st.session_state.get("nombre_usuario"):
        st.success(f"Hola, **{st.session_state.nombre_usuario}** 👋")
    else:
        st.info("Escribe tu nombre para poder marcar tareas")

    st.divider()
    if APP_URL:
        st.success(f"🌍 App pública:\n{APP_URL}")

    st.divider()
    if st.session_state.get("scheduler_started"):
        st.success("📲 Recordatorios activos")
        with st.expander("Ver horarios"):
            st.markdown("**🍽️ Comidas — notifica a TODOS**")
            for c in COMIDAS:
                st.markdown(f"**{c['label']}** `{c['hora_inicio']}–{c['hora_fin']}`  \n"
                            f"⏰ `{c['reminder']}` | 🚨 `{c['alerta']}`")
            st.markdown("**🎾 Recreativas / 🧹 Mantenimiento**")
            st.markdown("→ Solo responsable (todos si +5 días sin registrar)")
        st.divider()
        st.markdown("**🧪 Probar correo**")
        if st.button("Enviar correo de prueba"):
            send_email(
                "Prueba — Bolo y Pía funcionando ✅",
                f"Todo está funcionando correctamente.\nApp: {APP_URL}",
                tipo="test", solo_responsable=False)
            st.success("✅ Correo enviado a toda la familia")
    else:
        st.warning("⚠️ Scheduler no activo")

    st.divider()
    if not is_developer():
        with st.expander("🔑 Acceso developer"):
            dev_pwd = st.text_input("Contraseña:", type="password", key="dev_pwd_input")
            if st.button("Entrar", key="dev_login_btn"):
                if dev_pwd == DEVELOPER_PASSWORD:
                    st.session_state.is_developer = True
                    st.rerun()
                else:
                    st.error("❌ Contraseña incorrecta")
    else:
        st.success("🔓 Modo developer activo")
        if st.button("Cerrar sesión developer"):
            st.session_state.is_developer = False
            st.rerun()

# =====================================================
# HELPERS UI
# =====================================================
def render_comida(comida, df_hoy, nombre):
    estado   = get_estado_tarea(df_hoy, comida["key"])
    ya_hecha = bool(estado is not None and estado["completada"])
    activa   = ventana_activa(comida)
    with st.container(border=True):
        col1, col2 = st.columns([3, 1])
        with col1:
            badge = "🟢 Ventana abierta" if activa else f"🕐 {comida['hora_inicio']}–{comida['hora_fin']}"
            st.markdown(f"**{comida['label']}** &nbsp; `{badge}`")
            if ya_hecha:
                st.success(f"✅ Dado por **{estado['quien']}** a las {estado['timestamp'][11:16]}")
            elif activa:
                st.warning("⏳ Pendiente — ¡ventana abierta!")
            else:
                st.info("⏸️ Fuera de ventana — botón desactivado")
        with col2:
            if not ya_hecha:
                if activa:
                    if st.button("Marcar ✅", key=f"btn_{comida['key']}"):
                        if not nombre.strip():
                            st.error("⚠️ Escribe tu nombre en el sidebar")
                        else:
                            marcar_tarea(comida["key"], True, nombre.strip())
                            st.rerun()
                else:
                    st.button("Marcar ✅", key=f"btn_{comida['key']}", disabled=True,
                              help=f"Solo entre {comida['hora_inicio']} y {comida['hora_fin']}")
            else:
                if st.button("Deshacer", key=f"undo_{comida['key']}"):
                    if not nombre.strip():
                        st.error("⚠️ Escribe tu nombre en el sidebar")
                    else:
                        marcar_tarea(comida["key"], False, nombre.strip())
                        st.rerun()

# =====================================================
# HEADER
# =====================================================
st.markdown('<div class="hero-header"><span class="hero-paws">🐾</span></div>', unsafe_allow_html=True)
st.title("Bolo y Pía")
turno = get_turno_semana()
if turno:
    st.markdown(f'Responsable esta semana: <span class="responsable-badge">👤 {turno[0]}</span>',
                unsafe_allow_html=True)
st.markdown(f"*{datetime.now().strftime('%A, %d de %B de %Y — %H:%M')}*")

if not st.session_state.get("nombre_usuario"):
    st.warning("⚠️ Escribe tu nombre en el sidebar izquierdo para poder marcar tareas.")

st.divider()

if "nombre_usuario" not in st.session_state:
    st.session_state.nombre_usuario = ""

# =====================================================
# TABS
# =====================================================
tab_labels = ["🏠 Hoy", "🍽️ Comidas", "🎾 Recreativas", "🧹 Mantenimiento", "📦 Inventario", "📅 Turno"]
if is_developer():
    tab_labels.append("📊 Historial")
tabs = st.tabs(tab_labels)

TAB_HOY     = 0
TAB_COMIDAS = 1
TAB_RECREAT = 2
TAB_MANT    = 3
TAB_INV     = 4
TAB_TURNO   = 5
TAB_HIST    = 6 if is_developer() else None

# ── TAB: HOY ─────────────────────────────────────────
with tabs[TAB_HOY]:
    df_hoy      = get_tareas_hoy()
    todas_keys  = ([c["key"] for c in COMIDAS]
                   + [t["key"] for t in TAREAS_RECREATIVAS]
                   + [t["key"] for t in TAREAS_MANTENIMIENTO])
    completadas = len(df_hoy[df_hoy["completada"] == 1]) if not df_hoy.empty else 0
    total       = len(todas_keys)

    bajos = check_inventario_bajo()
    if bajos:
        st.warning(f"📦 Stock bajo: **{', '.join([p['producto'] for p in bajos])}** — actualiza el inventario")

    col_prog, col_btn = st.columns([4, 1])
    with col_prog:
        st.progress(completadas / total if total > 0 else 0,
                    text=f"{completadas}/{total} tareas completadas hoy")
    with col_btn:
        if st.button("🔄", help="Actualizar"):
            st.rerun()

    st.divider()

    def render_mini_status(keys, label):
        hechas  = sum(1 for k in keys if not df_hoy.empty and
                      not df_hoy[(df_hoy["tarea"] == k) & (df_hoy["completada"] == 1)].empty)
        total_k = len(keys)
        emoji   = "✅" if hechas == total_k else ("⚠️" if hechas > 0 else "🔴")
        st.markdown(f"{emoji} **{label}** — {hechas}/{total_k}")

    c1, c2, c3 = st.columns(3)
    with c1: render_mini_status([c["key"] for c in COMIDAS], "Comidas")
    with c2: render_mini_status([t["key"] for t in TAREAS_RECREATIVAS], "Recreativas")
    with c3: render_mini_status([t["key"] for t in TAREAS_MANTENIMIENTO], "Mantenimiento")

    st.divider()
    st.markdown("### ⚡ Acceso rápido")
    for comida in COMIDAS:
        estado   = get_estado_tarea(df_hoy, comida["key"])
        ya_hecha = bool(estado is not None and estado["completada"])
        activa   = ventana_activa(comida)
        col1, col2, col3 = st.columns([2, 2, 1])
        col1.markdown(f"**{comida['label']}**")
        if ya_hecha:
            col2.success(f"✅ {estado['quien']}")
        elif activa:
            col2.warning("⏳ Pendiente")
        else:
            col2.info(f"🕐 {comida['hora_inicio']}–{comida['hora_fin']}")
        with col3:
            if not ya_hecha and activa:
                if st.button("✅", key=f"quick_{comida['key']}"):
                    if not st.session_state.nombre_usuario.strip():
                        st.error("⚠️ Escribe tu nombre en el sidebar")
                    else:
                        marcar_tarea(comida["key"], True, st.session_state.nombre_usuario.strip())
                        st.rerun()

    if completadas == total:
        st.balloons()
        st.success("🎉 ¡Todo listo con Bolo y Pía hoy! Son unos perros muy bien cuidados ❤️")
    elif completadas == 0:
        st.error("🚨 Ninguna tarea completada aún. ¡Bolo y Pía te necesitan!")
    else:
        st.info(f"🐾 Van {completadas} de {total} tareas. ¡Casi!")

# ── TAB: COMIDAS ─────────────────────────────────────
with tabs[TAB_COMIDAS]:
    st.markdown("### 🍽️ Comidas del día")
    st.caption("El botón solo se activa durante la ventana horaria • Las alertas llegan a **toda la familia**")
    df_hoy_c = get_tareas_hoy()
    for comida in COMIDAS:
        render_comida(comida, df_hoy_c, st.session_state.nombre_usuario)

# ── TAB: RECREATIVAS ─────────────────────────────────
with tabs[TAB_RECREAT]:
    st.markdown("### 🎾 Tareas recreativas")
    st.caption("Alerta diaria al responsable • Si +5 días sin registrar → notifica a **toda la familia**")
    df_hoy_r = get_tareas_hoy()
    for tarea in TAREAS_RECREATIVAS:
        estado   = get_estado_tarea(df_hoy_r, tarea["key"])
        ya_hecha = bool(estado is not None and estado["completada"])
        sin_5    = tarea_sin_registrar_n_dias(tarea["key"], 5)
        with st.container(border=True):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{tarea['label']}**")
                st.caption(tarea["desc"])
                if sin_5 and not ya_hecha:
                    st.error("🚨 Sin registrar hace 5+ días — toda la familia será notificada")
                if ya_hecha:
                    st.success(f"✅ Hecho por **{estado['quien']}** a las {estado['timestamp'][11:16]}")
                    if estado["notas"]:
                        st.caption(f"📝 {estado['notas']}")
                else:
                    st.warning(f"⏳ Pendiente — alerta a las {tarea['alerta']}")
            with col2:
                tarea_key = tarea["key"]
                if not ya_hecha:
                    nota = ""
                    if tarea.get("duracion"):
                        dur  = st.selectbox("Duración:", ["15 min","30 min","45 min","1 hora","+1 hora"],
                                            key=f"dur_{tarea_key}")
                        nota = f"Duración: {dur}"
                    elif tarea.get("nota"):
                        nota = st.text_input("Nota:", key=f"nota_{tarea_key}",
                                             placeholder="¿Algo fuera de lo normal?")
                    if st.button("Marcar ✅", key=f"btn_{tarea_key}"):
                        if not st.session_state.nombre_usuario.strip():
                            st.error("⚠️ Escribe tu nombre en el sidebar")
                        else:
                            marcar_tarea(tarea_key, True, st.session_state.nombre_usuario.strip(), nota)
                            st.rerun()
                else:
                    if st.button("Deshacer", key=f"undo_{tarea_key}"):
                        if not st.session_state.nombre_usuario.strip():
                            st.error("⚠️ Escribe tu nombre en el sidebar")
                        else:
                            marcar_tarea(tarea_key, False, st.session_state.nombre_usuario.strip())
                            st.rerun()

# ── TAB: MANTENIMIENTO ───────────────────────────────
with tabs[TAB_MANT]:
    st.markdown("### 🧹 Mantenimiento")
    st.caption("Alerta diaria al responsable • Si +5 días sin registrar → notifica a **toda la familia**")
    df_hoy_m = get_tareas_hoy()
    for tarea in TAREAS_MANTENIMIENTO:
        estado   = get_estado_tarea(df_hoy_m, tarea["key"])
        ya_hecha = bool(estado is not None and estado["completada"])
        sin_5    = tarea_sin_registrar_n_dias(tarea["key"], 5)
        with st.container(border=True):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{tarea['label']}**")
                if sin_5 and not ya_hecha:
                    st.error("🚨 Sin registrar hace 5+ días — toda la familia será notificada")
                if ya_hecha:
                    st.success(f"✅ Hecho por **{estado['quien']}** a las {estado['timestamp'][11:16]}")
                    if estado["notas"]:
                        st.caption(f"📝 {estado['notas']}")
                else:
                    st.warning("⏳ Pendiente")
            with col2:
                tarea_key = tarea["key"]
                if not ya_hecha:
                    nota = ""
                    if tarea.get("nota"):
                        nota = st.text_input("Nota:", key=f"nota_{tarea_key}",
                                             placeholder="¿Algo fuera de lo normal?")
                    if st.button("Marcar ✅", key=f"btn_{tarea_key}"):
                        if not st.session_state.nombre_usuario.strip():
                            st.error("⚠️ Escribe tu nombre en el sidebar")
                        else:
                            marcar_tarea(tarea_key, True, st.session_state.nombre_usuario.strip(), nota)
                            st.rerun()
                else:
                    if st.button("Deshacer", key=f"undo_{tarea_key}"):
                        if not st.session_state.nombre_usuario.strip():
                            st.error("⚠️ Escribe tu nombre en el sidebar")
                        else:
                            marcar_tarea(tarea_key, False, st.session_state.nombre_usuario.strip())
                            st.rerun()

# ── TAB: INVENTARIO ──────────────────────────────────
with tabs[TAB_INV]:
    st.markdown("### 📦 Inventario de comida y suministros")
    df_inv = get_inventario()
    for cat_key, cat_info in INVENTARIO_CATEGORIAS.items():
        st.markdown(f"#### {cat_info['label']}")
        df_cat = df_inv[df_inv["categoria"] == cat_key] if not df_inv.empty else pd.DataFrame()
        if not df_cat.empty:
            for _, row in df_cat.iterrows():
                pct  = min(row["cantidad"] / max(row["minimo"] * 3, 1), 1.0)
                bajo = row["cantidad"] <= row["minimo"]
                col1, col2 = st.columns([3, 1])
                with col1:
                    emoji = "🔴" if bajo else "🟢"
                    st.markdown(f"{emoji} **{row['producto']}** — {row['cantidad']} {row['unidad']}")
                    st.progress(pct, text=f"Mínimo: {row['minimo']} {row['unidad']}")
                    if bajo:
                        st.error("⚠️ Stock bajo — reponer pronto")
                with col2:
                    act = row.get("actualizado", "")
                    st.caption(f"Actualizado:\n{str(act)[5:16] if act else 'N/A'}")
        else:
            st.info(f"Sin productos registrados en {cat_info['label']}")
        st.divider()

    st.markdown("### ✏️ Actualizar producto")
    with st.container(border=True):
        cat_opciones = {v["label"]: k for k, v in INVENTARIO_CATEGORIAS.items()}
        cat_label    = st.selectbox("Categoría:", list(cat_opciones.keys()))
        cat_key_sel  = cat_opciones[cat_label]
        prods_cat    = INVENTARIO_CATEGORIAS[cat_key_sel]["productos"]
        col1, col2   = st.columns(2)
        with col1:
            prod_opcion = st.selectbox("Producto:", prods_cat + ["Otro..."])
            producto    = st.text_input("Nombre del producto:") if prod_opcion == "Otro..." else prod_opcion
            cantidad    = st.number_input("Cantidad actual:", min_value=0.0, step=0.5, value=1.0)
        with col2:
            unidad = st.selectbox("Unidad:", ["kg", "bolsas", "latas", "piezas", "g"])
            minimo = st.number_input("Alerta cuando quede menos de:", min_value=0.0, step=0.5, value=0.5)
        if st.button("💾 Guardar", type="primary"):
            if producto and producto.strip():
                upsert_inventario(producto.strip(), cantidad, unidad, minimo, cat_key_sel)
                st.success(f"✅ {producto} actualizado — {cantidad} {unidad}")
                st.rerun()
            else:
                st.error("Escribe el nombre del producto")

    st.divider()
    if st.button("📲 Revisar y notificar si hay stock bajo"):
        bajos = check_inventario_bajo()
        if bajos:
            make_inventario_check_job()()
            st.warning(f"⚠️ Notificado sobre {len(bajos)} producto(s) con stock bajo")
        else:
            st.success("✅ Todo el inventario está bien")

# ── TAB: TURNO SEMANAL ───────────────────────────────
with tabs[TAB_TURNO]:
    st.markdown("### 📅 Turno semanal")
    semana    = semana_actual()
    MESES     = ["enero","febrero","marzo","abril","mayo","junio","julio",
                 "agosto","septiembre","octubre","noviembre","diciembre"]
    lunes_dt   = datetime.strptime(semana, "%Y-%m-%d")
    domingo_dt = lunes_dt + timedelta(days=6)
    semana_label = (f"lunes {lunes_dt.day} de {MESES[lunes_dt.month-1]}"
                    f" al domingo {domingo_dt.day} de {MESES[domingo_dt.month-1]}")
    st.markdown(f"**Semana actual:** {semana_label}")

    turno_actual = get_turno_semana(semana)
    if turno_actual:
        st.success(f"✅ Responsable: **{turno_actual[0]}**")
    else:
        st.warning("⚠️ Nadie asignado — los recordatorios van a todos")

    st.divider()
    st.markdown("### Asignar responsable")
    with st.container(border=True):
        responsable = st.selectbox("¿Quién es el responsable esta semana?", NOMBRES_FAMILIA)
        email_resp  = dict(FAMILIA)[responsable]
        if st.button("✅ Asignar turno", type="primary"):
            set_turno_semana(semana, responsable, email_resp)
            send_email(
                f"📅 Turno semanal asignado — Bolo y Pía",
                f"Hola {responsable}!\n\n"
                f"Esta semana eres el/la responsable del cuidado de Bolo y Pía.\n"
                f"Los recordatorios diarios te llegarán directamente a ti.\n"
                f"(Las alertas de comidas llegan a toda la familia)\n\nApp: {APP_URL}",
                tipo="turno_asignado", solo_responsable=False)
            st.success(f"✅ Turno asignado a **{responsable}** — se notificó a todos")
            st.rerun()

    st.divider()
    st.markdown("### Historial de turnos")
    docs_turnos = list(db().turno_semanal.find({}, {"_id": 0}).sort("semana", DESCENDING).limit(10))
    if docs_turnos:
        st.dataframe(pd.DataFrame(docs_turnos)[["semana","responsable"]],
                     use_container_width=True, hide_index=True)
    else:
        st.info("Aún no hay turnos registrados.")

# ── TAB: HISTORIAL (DEVELOPER) ───────────────────────
if is_developer() and TAB_HIST is not None:
    with tabs[TAB_HIST]:
        st.markdown("### 📊 Dashboard — Solo Developer")

        # Cargar datos de MongoDB
        docs_tareas  = list(db().tareas.find({}, {"_id": 0}))
        docs_email   = list(db().email_logs.find({}, {"_id": 0}).sort("timestamp", DESCENDING).limit(200))
        docs_turnos2 = list(db().turno_semanal.find({}, {"_id": 0}).sort("semana", ASCENDING))

        df_hist     = pd.DataFrame(docs_tareas)  if docs_tareas  else pd.DataFrame()
        df_email    = pd.DataFrame(docs_email)   if docs_email   else pd.DataFrame()
        df_turnos_h = pd.DataFrame(docs_turnos2) if docs_turnos2 else pd.DataFrame()

        if df_hist.empty:
            st.info("Aún no hay datos. Usa la app unos días y vuelve aquí.")
        else:
            df_hist["timestamp_dt"] = pd.to_datetime(df_hist["timestamp"])
            df_hist["hora"]         = df_hist["timestamp_dt"].dt.hour + df_hist["timestamp_dt"].dt.minute / 60
            df_hist["dia_semana"]   = df_hist["timestamp_dt"].dt.day_name()
            df_hist["fecha_dt"]     = pd.to_datetime(df_hist["fecha"])
            NOMBRE_TAREA = {"desayuno":"Desayuno","comida":"Comida","cena":"Cena",
                            "paseo":"Paseo","salud":"Salud","agua":"Agua","espacio":"Espacio"}
            df_hist["tarea_label"] = df_hist["tarea"].map(NOMBRE_TAREA).fillna(df_hist["tarea"])
            df_ok        = df_hist[df_hist["completada"] == 1]
            dias_activos = df_hist["fecha"].nunique()

            # KPIs
            st.markdown("#### 📈 Resumen general")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Días con actividad",   dias_activos)
            k2.metric("Acciones registradas", len(df_ok))
            k3.metric("Tipos de tarea",       df_hist["tarea"].nunique())
            k4.metric("Personas activas",     df_ok["quien"].nunique() if not df_ok.empty else 0)
            st.divider()

            # Ranking
            st.markdown("#### 🏆 Ranking — ¿Quién cuida más?")
            ranking = (df_ok.groupby("quien").size().reset_index(name="chores")
                       .sort_values("chores", ascending=False))
            cb, cp = st.columns([3, 2])
            with cb:
                st.bar_chart(ranking.set_index("quien")["chores"], use_container_width=True)
            with cp:
                fig1 = go.Figure(go.Pie(labels=ranking["quien"], values=ranking["chores"],
                                        hole=0.45, textinfo="label+percent"))
                fig1.update_layout(margin=dict(t=10,b=10,l=10,r=10), height=260, showlegend=False)
                st.plotly_chart(fig1, use_container_width=True)
            st.divider()

            # Cumplimiento por tarea
            st.markdown("#### 🍽️ Cumplimiento por tipo de tarea")
            cumpl = df_ok.groupby("tarea_label")["fecha"].nunique().reset_index(name="dias")
            cumpl["pct"] = (cumpl["dias"] / dias_activos * 100).round(1)
            cumpl = cumpl.sort_values("pct", ascending=True)
            cb2, cp2 = st.columns([3, 2])
            with cb2:
                st.bar_chart(cumpl.set_index("tarea_label")["pct"], use_container_width=True)
            with cp2:
                acc  = df_ok.groupby("tarea_label").size().reset_index(name="n")
                fig2 = go.Figure(go.Pie(labels=acc["tarea_label"], values=acc["n"],
                                        hole=0.45, textinfo="label+percent"))
                fig2.update_layout(margin=dict(t=10,b=10,l=10,r=10), height=280, showlegend=False)
                st.plotly_chart(fig2, use_container_width=True)
            st.divider()

            # Correlación turno vs cumplimiento
            if not df_turnos_h.empty and not df_ok.empty:
                st.markdown("#### 🗓️ Cumplimiento por responsable semanal")
                todas_keys_set  = set([c["key"] for c in COMIDAS]
                                       + [t["key"] for t in TAREAS_RECREATIVAS]
                                       + [t["key"] for t in TAREAS_MANTENIMIENTO])
                total_por_dia   = len(todas_keys_set)
                semana_stats    = []
                for _, row_turno in df_turnos_h.iterrows():
                    semana_str  = row_turno["semana"]
                    resp_nombre = row_turno["responsable"]
                    lunes_s     = pd.to_datetime(semana_str)
                    domingo_s   = lunes_s + timedelta(days=6)
                    mask        = (df_ok["fecha_dt"] >= lunes_s) & (df_ok["fecha_dt"] <= domingo_s)
                    df_sem      = df_ok[mask]
                    if df_sem.empty:
                        continue
                    dias_sem     = df_sem["fecha"].nunique()
                    tareas_hechas= len(df_sem)
                    max_pos      = dias_sem * total_por_dia
                    pct          = round(tareas_hechas / max_pos * 100, 1) if max_pos > 0 else 0
                    semana_stats.append({"semana": semana_str, "responsable": resp_nombre,
                                         "dias_activos": dias_sem, "tareas_completadas": tareas_hechas,
                                         "cumplimiento_%": pct})
                if semana_stats:
                    df_ss = pd.DataFrame(semana_stats)
                    resp_unicos = df_ss["responsable"].unique().tolist()
                    colores     = ["#f97316","#22d3a5","#3b82f6","#f43f5e","#a855f7","#fbbf24"]
                    color_map   = {r: colores[i % len(colores)] for i, r in enumerate(resp_unicos)}
                    fig_t = go.Figure()
                    for r in resp_unicos:
                        df_r = df_ss[df_ss["responsable"] == r]
                        fig_t.add_trace(go.Bar(x=df_r["semana"], y=df_r["cumplimiento_%"],
                                               name=r, marker_color=color_map[r],
                                               text=df_r["cumplimiento_%"].astype(str) + "%",
                                               textposition="outside"))
                    fig_t.update_layout(barmode="group",
                                        yaxis=dict(title="Cumplimiento (%)", range=[0, 115]),
                                        height=320, margin=dict(t=20,b=40,l=40,r=20),
                                        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                                        font=dict(color="#e8ecf4"))
                    st.plotly_chart(fig_t, use_container_width=True)
                    avg = (df_ss.groupby("responsable")["cumplimiento_%"].mean().round(1)
                           .reset_index().rename(columns={"cumplimiento_%": "Promedio (%)"})
                           .sort_values("Promedio (%)", ascending=False))
                    cols_avg = st.columns(min(len(avg), 5))
                    for i, (_, r) in enumerate(avg.iterrows()):
                        cols_avg[i % len(cols_avg)].metric(r["responsable"], f"{r['Promedio (%)']}%")
                    with st.expander("Ver tabla detallada"):
                        st.dataframe(df_ss, use_container_width=True, hide_index=True)
                else:
                    st.info("Aún no hay semanas con suficientes datos.")
                st.divider()

            # Duración de paseos
            df_paseos = df_ok[df_ok["tarea"] == "paseo"].copy() if not df_ok.empty else pd.DataFrame()
            if not df_paseos.empty and df_paseos["notas"].notna().any():
                st.markdown("#### 🦮 Duración de paseos")
                dur_counts = df_paseos["notas"].value_counts().reset_index()
                dur_counts.columns = ["Duración", "Veces"]
                st.bar_chart(dur_counts.set_index("Duración")["Veces"], use_container_width=True)
                st.divider()

            # Actividad por día de semana
            st.markdown("#### 📅 Actividad por día de la semana")
            orden   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            nombres = {"Monday":"Lunes","Tuesday":"Martes","Wednesday":"Miércoles",
                       "Thursday":"Jueves","Friday":"Viernes","Saturday":"Sábado","Sunday":"Domingo"}
            act_sem = (df_ok.groupby("dia_semana").size().reindex(orden, fill_value=0)
                       .rename(index=nombres).reset_index())
            act_sem.columns = ["dia", "acciones"]
            st.bar_chart(act_sem.set_index("dia")["acciones"], use_container_width=True)
            st.divider()

            # A tiempo vs con recordatorio
            st.markdown("#### ⏰ Comidas — ¿A tiempo o con recordatorio?")
            VENTANAS = {"desayuno": (7.0, 11.0), "comida": (13.0, 15.0), "cena": (19.0, 21.0)}
            res_tiempo = []
            for key, (h_ini, h_fin) in VENTANAS.items():
                df_c = df_ok[df_ok["tarea"] == key] if not df_ok.empty else pd.DataFrame()
                if df_c.empty: continue
                a_tiempo = int(df_c["hora"].between(h_ini, h_fin).sum())
                res_tiempo.append({"Comida": NOMBRE_TAREA.get(key, key),
                                   "A tiempo ✅": a_tiempo,
                                   "Con recordatorio ⏰": len(df_c) - a_tiempo})
            if res_tiempo:
                df_t = pd.DataFrame(res_tiempo).set_index("Comida")
                cb3, cp3 = st.columns([3, 2])
                with cb3:
                    st.bar_chart(df_t, use_container_width=True)
                with cp3:
                    fig3 = go.Figure(go.Pie(
                        labels=["A tiempo ✅","Con recordatorio ⏰"],
                        values=[int(df_t["A tiempo ✅"].sum()), int(df_t["Con recordatorio ⏰"].sum())],
                        hole=0.45, marker_colors=["#2ecc71","#e67e22"], textinfo="label+percent"))
                    fig3.update_layout(margin=dict(t=10,b=10,l=10,r=10), height=250, showlegend=False)
                    st.plotly_chart(fig3, use_container_width=True)
            st.divider()

            # Racha de días perfectos
            st.markdown("#### 🔥 Racha de días perfectos")
            todas_keys_list = set([c["key"] for c in COMIDAS]
                                   + [t["key"] for t in TAREAS_RECREATIVAS]
                                   + [t["key"] for t in TAREAS_MANTENIMIENTO])
            dias_perfectos  = sorted([pd.to_datetime(f) for f, grp in df_ok.groupby("fecha")
                                       if todas_keys_list.issubset(set(grp["tarea"].tolist()))])
            temp = racha_max = racha_actual = 0
            for i, f in enumerate(dias_perfectos):
                temp = temp + 1 if i == 0 or (f - dias_perfectos[i-1]).days == 1 else 1
                racha_max = max(racha_max, temp)
            hoy_dt = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
            if dias_perfectos and (hoy_dt - dias_perfectos[-1]).days <= 1:
                racha_actual = temp
            r1, r2, r3 = st.columns(3)
            r1.metric("🔥 Racha actual",   f"{racha_actual} días")
            r2.metric("🏅 Racha máxima",   f"{racha_max} días")
            r3.metric("✅ Días perfectos",  len(dias_perfectos))
            st.divider()

            # Email log
            st.markdown("#### 📧 Log de correos enviados")
            if df_email.empty:
                st.info("No se han enviado correos aún.")
            else:
                enviados = len(df_email[df_email["enviado"] == True]) if "enviado" in df_email.columns else 0
                w1, w2, w3 = st.columns(3)
                w1.metric("Total", len(df_email))
                w2.metric("Enviados ✅", enviados)
                w3.metric("Fallos ❌", len(df_email) - enviados)
                with st.expander("Ver log completo"):
                    st.dataframe(df_email, use_container_width=True)
            st.divider()

            # Exportar JSON
            st.markdown("#### 📥 Exportar historial completo (JSON)")
            if st.button("🔄 Generar JSON", type="primary"):
                export_data = {
                    "exportado_en": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "version": "3.1-gmail",
                    "tareas":       list(db().tareas.find({}, {"_id": 0})),
                    "historial":    list(db().historial.find({}, {"_id": 0})),
                    "email_logs":   list(db().email_logs.find({}, {"_id": 0})),
                    "inventario":   list(db().inventario.find({}, {"_id": 0})),
                    "turno_semanal":list(db().turno_semanal.find({}, {"_id": 0})),
                }
                json_str = json.dumps(export_data, ensure_ascii=False, indent=2, default=str)
                st.download_button(
                    label="📥 Descargar historial.json",
                    data=json_str,
                    file_name=f"boloypía_historial_{datetime.now().strftime('%Y%m%d_%H%M')}.json",
                    mime="application/json",
                )
                st.success(f"✅ JSON listo — {len(export_data['tareas'])} tareas")
                with st.expander("Preview"):
                    st.json({"version": export_data["version"],
                             "total_tareas": len(export_data["tareas"]),
                             "total_emails": len(export_data["email_logs"])})

            if st.button("🔄 Actualizar dashboard"):
                st.rerun()

# =====================================================
# PIE
# =====================================================
st.caption("🐾 App familiar Bolo y Pía — MongoDB Atlas + Streamlit Cloud + notificaciones por Gmail")
