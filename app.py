"""
Omer Outreach Tool — CRM-Viewer + Kommentar-Generator für LinkedIn
Omer kopiert Namen rein → App zeigt die Nachrichten aus Airtable in gleicher Reihenfolge.
Omer postet einen LinkedIn-Post → App generiert passende Kommentare.

Workflow: Koren generiert Nachrichten via /omer Skill (Claude) → Airtable → Omer öffnet diese App

Usage: streamlit run execution/omer_app.py
"""

import streamlit as st
import json
import ssl
import urllib.request
import urllib.parse
import os
import re
import base64
import io
from streamlit_paste_button import paste_image_button as pbutton

# --- Config ---
# Support both .env (local) and Streamlit secrets (cloud)
def get_secret(key, default=""):
    # 1. Streamlit secrets (cloud deployment)
    try:
        return st.secrets[key]
    except Exception:
        pass
    # 2. Environment variable
    val = os.getenv(key, "")
    if val:
        return val
    # 3. Try loading .env file (local only)
    try:
        from dotenv import load_dotenv
        load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
        return os.getenv(key, default)
    except ImportError:
        return default


AIRTABLE_TOKEN = get_secret("AIRTABLE_API_TOKEN")
BASE_ID = "appz5XrcUwpc6NnG5"
TABLE_ID = "tblsrlDRFtnLPimP6"
GEMINI_API_KEY = get_secret("GEMINI_API_KEY")

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()


# --- Airtable Helpers ---

def airtable_request(method, path, data=None):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}{path}"
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {AIRTABLE_TOKEN}")
    if body:
        req.add_header("Content-Type", "application/json")
    resp = urllib.request.urlopen(req, context=SSL_CTX, timeout=30)
    return json.loads(resp.read())


def update_lead_in_airtable(record_id, fields):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_ID}/{record_id}"
    body = json.dumps({"fields": fields}).encode()
    req = urllib.request.Request(url, data=body, method="PATCH")
    req.add_header("Authorization", f"Bearer {AIRTABLE_TOKEN}")
    req.add_header("Content-Type", "application/json")
    urllib.request.urlopen(req, context=SSL_CTX, timeout=30)


def mark_as_sent(record_id):
    update_lead_in_airtable(record_id, {"Nachricht Status": "Gesendet"})


def get_all_leads_with_messages():
    """Load all leads that have a personalized message."""
    formula = 'NOT({Personalisierte Nachricht}=BLANK())'
    all_records = []
    offset = None
    while True:
        params = [
            ("filterByFormula", formula),
            ("fields[]", "Name"), ("fields[]", "Vorname"), ("fields[]", "Nachname"),
            ("fields[]", "Firma"), ("fields[]", "Position"),
            ("fields[]", "Personalisierte Nachricht"), ("fields[]", "Nachricht Status"),
            ("pageSize", "100"),
        ]
        if offset:
            params.append(("offset", offset))
        result = airtable_request("GET", f"?{urllib.parse.urlencode(params)}")
        all_records.extend(result.get("records", []))
        offset = result.get("offset")
        if not offset:
            break
    return all_records


def get_stats():
    all_records = []
    offset = None
    while True:
        params = [("fields[]", "Nachricht Status"), ("pageSize", "100")]
        if offset:
            params.append(("offset", offset))
        result = airtable_request("GET", f"?{urllib.parse.urlencode(params)}")
        all_records.extend(result.get("records", []))
        offset = result.get("offset")
        if not offset:
            break
    stats = {"Gesamt": len(all_records), "Entwurf": 0, "Zugewiesen": 0, "Gesendet": 0, "Beantwortet": 0}
    for r in all_records:
        status = r.get("fields", {}).get("Nachricht Status", "")
        if status in stats:
            stats[status] += 1
    return stats


def parse_names_from_text(text):
    """Extract names from pasted LinkedIn connections text."""
    lines = [l.strip() for l in text.strip().split("\n") if l.strip()]
    names = []
    skip_words = {"ago", "day", "days", "week", "weeks", "month", "months", "year", "years",
                  "connected", "mutual", "connection", "connections", "message", "pending",
                  "follow", "remove", "more", "degree", "1st", "2nd", "3rd"}

    for line in lines:
        lower = line.lower().strip()
        if any(lower.startswith(w) for w in skip_words):
            continue
        if re.match(r'^\d+', lower):
            continue
        if len(lower) < 3:
            continue

        name = re.split(r'[,|·•\-–—]', line)[0].strip()
        words = name.split()
        if len(words) > 4:
            name = " ".join(words[:3])
        if len(words) >= 2 and len(name) >= 3:
            names.append(name)

    return names


def match_names_to_leads(names, all_leads):
    """Match pasted names to Airtable records. Return in same order as names."""
    lookup = {}
    for rec in all_leads:
        f = rec.get("fields", {})
        full_name = f.get("Name", "").strip().lower()
        nachname = f.get("Nachname", "").strip().lower()
        vorname = f.get("Vorname", "").strip().lower()
        if full_name:
            lookup[full_name] = rec
        if nachname:
            if nachname not in lookup:
                lookup[nachname] = rec
        if vorname and nachname:
            lookup[f"{vorname} {nachname}"] = rec

    matched = []
    not_found = []

    for name in names:
        name_lower = name.lower().strip()
        found = None

        if name_lower in lookup:
            found = lookup[name_lower]

        if not found:
            for key, rec in lookup.items():
                rec_name = rec.get("fields", {}).get("Name", "").lower()
                if name_lower in rec_name or rec_name in name_lower:
                    found = rec
                    break
                name_parts = name_lower.split()
                if len(name_parts) >= 2 and name_parts[-1] == key:
                    found = rec
                    break

        if found:
            matched.append({"name": name, "record": found})
        else:
            not_found.append(name)

    return matched, not_found


# --- Gemini API (for screenshots AND comment generation) ---

def gemini_request(prompt, image_bytes=None, media_type="image/png", max_tokens=2000, temperature=0.2):
    """Universal Gemini API call — works for text-only and image+text."""
    if not GEMINI_API_KEY:
        return None, "GEMINI_API_KEY nicht gesetzt"

    parts = []
    if image_bytes:
        b64_image = base64.b64encode(image_bytes).decode("utf-8")
        parts.append({
            "inline_data": {
                "mime_type": media_type,
                "data": b64_image,
            },
        })
    parts.append({"text": prompt})

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": temperature,
        },
    }

    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key={GEMINI_API_KEY}"
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json")
        resp = urllib.request.urlopen(req, context=SSL_CTX, timeout=90)
        result = json.loads(resp.read())
        candidates = result.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            text = parts[0].get("text", "") if parts else ""
            return text, None
        return None, "Keine Antwort von Gemini"
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="ignore")[:300]
        return None, f"Gemini API Fehler {e.code}: {body}"
    except Exception as e:
        return None, str(e)


def get_media_type(filename):
    ext = os.path.splitext(filename or "")[1].lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
    }.get(ext, "image/png")


SCREENSHOT_PROMPT_NAMES = """Analysiere diesen LinkedIn-Screenshot.
Extrahiere ALLE Personen-Namen die du siehst.

Fuer JEDE Person gib aus:
- Name (Vor- und Nachname)
- Position/Headline (falls sichtbar)
- Firma (falls sichtbar)

Format (eine Person pro Zeile):
Name | Position | Firma

Gib NUR die Liste aus, nichts anderes. Wenn du keine Namen findest, schreib "KEINE NAMEN GEFUNDEN"."""

SCREENSHOT_PROMPT_POST = """Analysiere diesen LinkedIn-Post Screenshot.

Extrahiere:
1. POSTER_NAME: Wer hat den Post geschrieben? (Vor- und Nachname)
2. POSTER_HEADLINE: Was steht unter dem Namen? (Position, Firma etc.)
3. POST_TEXT: Der komplette Text des Posts

Format (genau so):
POSTER_NAME: [Name]
POSTER_HEADLINE: [Headline]
POST_TEXT:
[Der komplette Post-Text]

Gib NUR diese Infos aus, nichts anderes."""

SCREENSHOT_PROMPT_MULTI_POST = """Analysiere diesen LinkedIn-Feed Screenshot. Es koennen MEHRERE Posts sichtbar sein.

Fuer JEDEN sichtbaren Post extrahiere:
1. POSTER_NAME: Wer hat den Post geschrieben?
2. POSTER_HEADLINE: Position/Firma unter dem Namen
3. POST_TEXT: Der Text des Posts (so viel wie sichtbar)

Format (genau so, fuer JEDEN Post):
---POST---
POSTER_NAME: [Name]
POSTER_HEADLINE: [Headline]
POST_TEXT:
[Post-Text]

Trenne jeden Post mit ---POST---. Gib NUR die Posts aus, nichts anderes.
Wenn nur 1 Post sichtbar ist, gib nur den einen aus."""


def parse_multi_posts_from_screenshot(analysis_text):
    """Parse multiple posts from feed screenshot analysis."""
    if not analysis_text:
        return []
    posts = []
    chunks = analysis_text.split("---POST---")
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk:
            continue
        post = {"name": "", "headline": "", "text": ""}
        lines = chunk.split("\n")
        in_post_text = False
        post_lines = []
        for line in lines:
            if line.startswith("POSTER_NAME:"):
                post["name"] = line.replace("POSTER_NAME:", "").strip()
            elif line.startswith("POSTER_HEADLINE:"):
                post["headline"] = line.replace("POSTER_HEADLINE:", "").strip()
            elif line.startswith("POST_TEXT:"):
                in_post_text = True
            elif in_post_text:
                post_lines.append(line)
        post["text"] = "\n".join(post_lines).strip()
        if post["text"] and post["name"]:
            posts.append(post)
    return posts


def parse_names_from_screenshot(analysis_text):
    if not analysis_text or "KEINE NAMEN" in analysis_text:
        return []
    names = []
    for line in analysis_text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
        if parts and len(parts[0].split()) >= 2:
            names.append(parts[0].strip())
    return names


def parse_post_from_screenshot(analysis_text):
    result = {"name": "", "headline": "", "text": ""}
    if not analysis_text:
        return result

    lines = analysis_text.strip().split("\n")
    in_post_text = False
    post_lines = []

    for line in lines:
        if line.startswith("POSTER_NAME:"):
            result["name"] = line.replace("POSTER_NAME:", "").strip()
        elif line.startswith("POSTER_HEADLINE:"):
            result["headline"] = line.replace("POSTER_HEADLINE:", "").strip()
        elif line.startswith("POST_TEXT:"):
            in_post_text = True
        elif in_post_text:
            post_lines.append(line)

    result["text"] = "\n".join(post_lines).strip()
    return result


# --- Message Generator (auto-generate if no message in Airtable) ---

FILM_LABOR_CONTEXT = """Film-labor ist eine Filmproduktionsfirma aus Deutschland.
Was wir machen: B2B-Recruiting-Videos, Image-Filme, Produktvideos.
Ergebnis bei Kunden: 40% mehr qualifizierte Bewerbungen, professionelle Arbeitgebermarke.
Pakete: Basic (1 Video, halber Drehtag), Standard (3 Videos, 1 Drehtag), Premium (5+ Videos, 2 Drehtage).
Branchen-Fokus: Maschinenbau, Chemie, B2B allgemein.
Tonalitaet: Direkt, ehrlich, keine Corporate-Floskeln. Du-Form auf LinkedIn."""


def build_message_prompt(lead_fields):
    """Build prompt to generate a personalized LinkedIn opener message."""
    vorname = lead_fields.get("Vorname", "") or lead_fields.get("Name", "").split()[0] if lead_fields.get("Name") else "du"
    position = lead_fields.get("Position", "")
    firma = lead_fields.get("Firma", "")
    branche = lead_fields.get("Branche", "")
    firma_desc = lead_fields.get("Firmenbeschreibung", "")
    pains = lead_fields.get("Wie wir helfen koennen", "")

    return f"""Du bist Koren. Du tippst grade schnell eine LinkedIn Nachricht am Handy an {vorname}.

Du bist Filmemacher, hast ne Firma die Recruiting und Imagevideos fuer B2B Kunden produziert. Du hast dich mit {vorname} connected und willst einfach ins Gespraech kommen. KEIN Verkauf.

WAS DU WEISST:
Position: {position}
Firma: {firma}
Branche: {branche}
Firmeninfo: {firma_desc[:500]}
Analyse: {pains[:300]}

{FILM_LABOR_CONTEXT}

SCHREIB DIE NACHRICHT JETZT.

WICHTIG - SO TIPPST DU:
- Max 2-3 saetze. Unter 250 zeichen. Weniger ist mehr.
- Du tippst am handy. Kurz. Knapp. Wie ne whatsapp nachricht an nen business kontakt.
- Kleine natuerliche imperfektionen: mal n fehlender punkt am ende, mal "hab" statt "habe", "nem" statt "einem", "grad" statt "gerade"
- KEINE gedankenstriche. KEINE gaensefuesschen. KEINE perfekte interpunktion.
- KEINE verschachtelten saetze mit kommas. Wenn du ein komma brauchst mach 2 saetze draus.
- Eine idee pro nachricht. EINE. Nicht drei.
- Schreib wie 3. klasse lese-niveau. Einfache worte. Kurze saetze.

WAS DU SAGST:
- Satz 1: was dir an der firma/person aufgefallen ist (konkret, zeigt dass du geschaut hast)
- Satz 2: ne frage die sie beantworten WOLLEN weil es ihr thema ist

VERBOTEN (sofort als spam erkannt):
- "Danke fuers vernetzen" oder "schoen dass wir connected sind"
- "Ich bin Koren von Film-labor" (weiss er schon)
- irgendwas mit "wir helfen" oder "wir machen videos"
- "spannend" "beeindruckt" "freue mich" "ich hoffe" "interessant"
- gedankenstriche
- woerter in anfuehrungszeichen setzen
- termin vorschlagen
- emojis
- mehr als 250 zeichen

SO KLINGTS RICHTIG:
"hey Jo, buefa beliefert ja halb norddeutschland mit chemie rohstoffen. wie erklaert ihr bewerbern eigentlich was ihr genau macht?"
"Markus, sehe grad ihr sucht 5 leute im engineering. kommen genug gute bewerbungen rein oder isses eher zaeh?"

Gib NUR die nachricht aus. Nix davor nix danach."""


def validate_message(message, lead_fields):
    """Second Gemini call: validate message against framework rules and facts."""
    vorname = lead_fields.get("Vorname", "") or lead_fields.get("Name", "").split()[0] if lead_fields.get("Name") else ""
    firma = lead_fields.get("Firma", "")
    position = lead_fields.get("Position", "")
    branche = lead_fields.get("Branche", "")
    firma_desc = lead_fields.get("Firmenbeschreibung", "")

    prompt = f"""Du bist ein strenger Qualitaetspruefer fuer LinkedIn-Nachrichten.

NACHRICHT ZUM PRUEFEN:
"{message}"

BEKANNTE FAKTEN UEBER DIE PERSON (aus unserem CRM):
- Name: {vorname}
- Firma: {firma}
- Position: {position}
- Branche: {branche}
- Firmeninfo: {firma_desc[:400]}

PRUEFE DIESE REGELN:

1. FAKTENCHECK: Erwaehnt die Nachricht Firmennamen, Produkte, Events, Insolvenzen, Zahlen oder andere spezifische Fakten die NICHT in den bekannten CRM-Daten stehen? Wenn ja = FAIL (moeglicherweise halluziniert!)
2. LAENGE: Unter 250 Zeichen? Wenn nein = FAIL
3. VERBOTENE WOERTER: Enthaelt "danke fuers vernetzen", "ich bin Koren", "wir helfen", "wir machen videos", "spannend", "beeindruckt", "freue mich", "ich hoffe", "interessant"? Wenn ja = FAIL
4. GEDANKENSTRICHE/EMOJIS: Enthaelt Gedankenstriche (—, –) oder Emojis? Wenn ja = FAIL
5. ANFUEHRUNGSZEICHEN: Setzt Woerter in Anfuehrungszeichen? Wenn ja = FAIL
6. TON: Klingt es natuerlich wie am Handy getippt? Oder zu perfekt/AI-artig?

ANTWORT FORMAT (genau so):
STATUS: OK oder FAIL
PROBLEME: [Liste der Probleme, oder "keine"]
VORSCHLAG: [Verbesserte Version falls FAIL, oder "keiner"]"""

    result, error = gemini_request(prompt, max_tokens=800, temperature=0.1)
    if error or not result:
        return {"status": "UNKNOWN", "problems": "Validierung fehlgeschlagen", "suggestion": ""}

    status = "OK" if "STATUS: OK" in result or "STATUS:OK" in result else "FAIL"
    problems = ""
    suggestion = ""

    for line in result.split("\n"):
        if line.startswith("PROBLEME:"):
            problems = line.replace("PROBLEME:", "").strip()
        elif line.startswith("VORSCHLAG:"):
            suggestion = line.replace("VORSCHLAG:", "").strip()

    # Also extract multi-line suggestion
    if "VORSCHLAG:" in result:
        parts = result.split("VORSCHLAG:")
        if len(parts) > 1:
            suggestion = parts[1].strip()
            # Clean up quotes
            if suggestion.startswith('"') and suggestion.endswith('"'):
                suggestion = suggestion[1:-1]

    return {"status": status, "problems": problems, "suggestion": suggestion}


def generate_message_for_lead(lead_fields):
    """Generate + validate a personalized message via Gemini. Returns (message, validation) tuple."""
    prompt = build_message_prompt(lead_fields)

    # Try up to 2 times: generate → validate → if FAIL, use suggestion or regenerate
    for attempt in range(2):
        text, error = gemini_request(prompt, max_tokens=500, temperature=0.8)
        if error or not text:
            return None, None

        # Clean up quotes
        text = text.strip()
        if text.startswith('"') and text.endswith('"'):
            text = text[1:-1]
        if text.startswith('\u201e') and text.endswith('\u201c'):
            text = text[1:-1]
        text = text.strip()

        # Validate
        validation = validate_message(text, lead_fields)

        if validation["status"] == "OK":
            return text, validation

        # If FAIL and we have a suggestion, use it on first attempt
        if attempt == 0 and validation.get("suggestion") and validation["suggestion"] != "keiner":
            suggested = validation["suggestion"].strip()
            if len(suggested) > 20:
                # Validate the suggestion too
                val2 = validate_message(suggested, lead_fields)
                if val2["status"] == "OK":
                    return suggested, val2
            # Otherwise try again with a fresh generation

    # Return last attempt with its validation (even if FAIL — user decides)
    return text, validation


def save_message_to_airtable(record_id, message):
    """Save generated message to Airtable."""
    update_lead_in_airtable(record_id, {
        "Personalisierte Nachricht": message,
        "Nachricht Status": "Entwurf",
    })


# --- Comment Generator ---

def find_lead_by_name(name):
    """Search Airtable for a lead by name."""
    parts = name.strip().split()
    if not parts:
        return None
    nachname = parts[-1]
    formula = f'FIND(LOWER("{nachname}"), LOWER({{Name}}))'
    fields = [
        "Name", "Vorname", "Nachname", "Position", "Firma", "Branche",
        "Firmenbeschreibung", "Nachricht Status", "Conversation Status",
        "Wie wir helfen koennen",
    ]
    params = urllib.parse.urlencode(
        [("filterByFormula", formula)] + [("fields[]", f) for f in fields]
    )
    try:
        result = airtable_request("GET", f"?{params}")
        if result and result.get("records"):
            return result["records"][0]
    except Exception:
        pass
    return None


def classify_poster(name, airtable_record=None, poster_info=""):
    if airtable_record:
        status = airtable_record.get("fields", {}).get("Conversation Status", "")
        if status in ("Termin", "Abschluss", "Kunde"):
            return "KUNDE"
        return "PROSPECT"

    info_lower = (poster_info + " " + name).lower()
    influencer_signals = [
        "creator", "influencer", "thought leader", "keynote",
        "bestselling author", "top voice", "linkedin top",
        "speaker", "100k", "50k", "followers",
    ]
    if any(s in info_lower for s in influencer_signals):
        return "INFLUENCER"

    peer_signals = [
        "videoproduktion", "filmproduktion", "content creator",
        "videoagentur", "kreativagentur", "werbefilm", "imagefilm",
        "videographer", "filmmaker", "regisseur", "kameramann",
        "marketing agentur", "social media agentur",
    ]
    if any(s in info_lower for s in peer_signals):
        return "PEER"

    return "UNKNOWN"


CATEGORY_LABELS = {
    "PROSPECT": ("Prospect (Potentieller Kunde)", "🎯"),
    "INFLUENCER": ("Influencer", "⭐"),
    "PEER": ("Branchenkollege", "🤝"),
    "KUNDE": ("Bestehender Kontakt", "💼"),
    "UNKNOWN": ("Unbekannt", "❓"),
}


def build_comment_prompt(post_text, poster_name, category, lead_data=None):
    lead_context = ""
    if lead_data:
        f = lead_data.get("fields", {})
        lead_context = f"""
LEAD-DATEN (Person ist ein PROSPECT in unserem CRM):
- Position: {f.get('Position', '?')}
- Firma: {f.get('Firma', '?')}
- Branche: {f.get('Branche', '?')}
- Firmeninfo: {f.get('Firmenbeschreibung', '?')[:300]}
- Wie wir helfen koennen: {f.get('Wie wir helfen koennen', '?')[:200]}
"""

    category_instructions = {
        "PROSPECT": """KATEGORIE: PROSPECT (potentieller Kunde!)
ZIEL: Zeig dass du Ahnung hast, bleib im Kopf. KEIN Pitch, KEIN Verkauf.
ANSAETZE:
- Zeig dass du das Problem aus der Praxis kennst
- Stell eine kluge Frage die zeigt dass du mitdenkst
- Teil eine kurze eigene Erfahrung (Video/Content-Bereich)
Das ist Warm-Up — du willst nur auf dem Radar sein.""",
        "INFLUENCER": """KATEGORIE: INFLUENCER (grosse Reichweite)
ZIEL: Klug mitreden — hier lesen viele Leute mit.
ANSAETZE:
- Ergaenze eine eigene Perspektive oder Gegen-Erfahrung
- Widersprich respektvoll mit einer eigenen Beobachtung
- Stell eine weiterführende Frage
Dein Kommentar ist deine Visitenkarte fuer alle die mitlesen.""",
        "PEER": """KATEGORIE: PEER (Branchenkollege)
ZIEL: Kollegial mitreden, Netzwerk staerken.
ANSAETZE:
- Zeig dass du den Content verfolgst
- Starte eine echte Diskussion
Locker, auf Augenhoehe.""",
        "KUNDE": """KATEGORIE: BESTEHENDER KONTAKT
ZIEL: In Erinnerung bleiben, Beziehung pflegen.
ANSAETZE:
- Direkt Mehrwert liefern oder eigene Erfahrung teilen
- Warm und persoenlich kommentieren
Wie ein Kumpel der mitliest.""",
        "UNKNOWN": """KATEGORIE: UNBEKANNT
ZIEL: Einfach was Kluges beitragen.
ANSAETZE:
- Eigene Erfahrung aus dem Video/Content-Bereich teilen
- Neugierige Frage stellen
Zeig Fachwissen ohne zu pushen.""",
    }

    # Name usage: only for PROSPECT and INFLUENCER, and only first name
    use_name = category in ("PROSPECT", "INFLUENCER", "KUNDE")
    name_instruction = ""
    if use_name and poster_name and poster_name != "Unbekannt":
        first_name = poster_name.split()[0] if poster_name else ""
        name_instruction = f"Du KANNST den Vornamen '{first_name}' nutzen (aber nicht in jedem Kommentar, maximal bei 1-2 von 3 Optionen)."
    else:
        name_instruction = "Benutze KEINEN Namen — du kennst die Person nicht persoenlich."

    return f"""Du bist Koren, 25, Filmemacher aus Frankfurt (Film-labor). Du kommentierst LinkedIn-Posts am Handy.

DEIN STIL: Locker, direkt, neugierig. Wie ein junger Kreativer der mitredet — NICHT wie ein Unternehmensberater.
Beispiele fuer deinen Ton:
- "Das deckt sich mit dem was wir bei Drehs sehen — die meisten unterschaetzen wie viel ein authentisches 30-Sekunden-Video bringt. Habt ihr das intern produziert?"
- "Spannend. Wir merken gerade den gleichen Shift bei unseren Kunden. Was hat bei euch den Ausschlag gegeben?"
- "Guter Punkt. Gerade im Mittelstand wird das noch komplett verschlafen. Wie reagieren eure Kunden darauf?"

POSTER: {poster_name}
{name_instruction}
{category_instructions.get(category, category_instructions['UNKNOWN'])}

{lead_context}

DER POST (Inhalt):
---
{post_text[:2000]}
---

GENERIERE GENAU 3 KOMMENTAR-OPTIONEN. Jede Option ist ein ANDERER Ansatz.

KOMMENTAR-REGELN:
- KURZ: 20-50 Woerter (2-3 Saetze MAX — wie eine schnelle Handy-Antwort)
- 100% spezifisch auf DIESEN Post (nie generisch wiederverwendbar)
- Mit einer Frage oder offenem Gedanken enden
- Schreib wie du WIRKLICH reden wuerdest (kein Corporate-Deutsch, kein "Exzellenter Punkt", kein "Spannender Beitrag")
- ECHTE Umlaute (ae→ä, oe→ö, ue→ü)
- Maximal 1 Emoji (oder keins)
- VERBOTEN: "Toller Beitrag!", "Danke fuers Teilen!", "100% agree!", Links, Eigenwerbung, Pitch

FORMAT (genau so):

OPTION A | [Formel-Name]
[Der Kommentar]

OPTION B | [Formel-Name]
[Der Kommentar]

OPTION C | [Formel-Name]
[Der Kommentar]

NUR die 3 Optionen. Nichts davor, nichts danach."""


def generate_comments(prompt):
    """Generate comments via Gemini API (free, cloud-compatible)."""
    return gemini_request(prompt, max_tokens=1500, temperature=0.7)


def validate_comment(comment):
    """Quick validation of a single comment."""
    issues = []
    if len(comment) < 30:
        issues.append("Zu kurz (unter 30 Zeichen)")
    words = comment.split()
    if len(words) < 10:
        issues.append("Unter 10 Woerter")
    if len(words) > 60:
        issues.append("Zu lang (ueber 60 Woerter)")
    banned = ["toller beitrag", "danke fuers teilen", "100% agree", "super post",
              "film-labor", "film labor", "filmlabor", "recruiting-video", "imagefilm"]
    lower = comment.lower()
    for b in banned:
        if b in lower:
            issues.append(f"Verbotenes Wort/Phrase: '{b}'")
    if comment.count("http") > 0:
        issues.append("Enthaelt Link (verboten in Kommentaren)")
    if not comment.rstrip().endswith("?"):
        issues.append("Endet nicht mit einer Frage (Fragen foerdern Antworten)")
    return issues


def parse_comment_options(raw_text):
    options = []
    parts = re.split(r'OPTION\s+([A-C])\s*\|\s*', raw_text)
    i = 1
    while i < len(parts) - 1:
        label = parts[i].strip()
        content = parts[i + 1].strip()
        lines = content.split("\n", 1)
        formula = lines[0].strip() if lines else ""
        comment = lines[1].strip() if len(lines) > 1 else content
        comment = comment.strip()
        options.append({
            "label": f"Option {label}",
            "formula": formula,
            "comment": comment,
        })
        i += 2
    return options


# --- UI ---

st.set_page_config(page_title="Omer Tool", page_icon="💬", layout="centered")

st.markdown("""
<style>
    .message-box {
        background: #f0f2f6;
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }
    .lead-name {
        font-weight: bold;
        font-size: 18px;
    }
    .lead-info {
        color: #666;
        font-size: 14px;
        margin-top: 2px;
    }
    .sent-badge {
        background: #d4edda;
        color: #155724;
        border-radius: 5px;
        padding: 3px 10px;
        font-size: 12px;
        font-weight: bold;
    }
    .skip-badge {
        background: #fff3cd;
        color: #856404;
        border-radius: 5px;
        padding: 3px 10px;
        font-size: 12px;
        font-weight: bold;
    }
    .notfound-badge {
        background: #f8d7da;
        color: #721c24;
        border-radius: 5px;
        padding: 3px 10px;
        font-size: 12px;
    }
    .crm-badge {
        background: #d1ecf1;
        color: #0c5460;
        border-radius: 5px;
        padding: 3px 10px;
        font-size: 11px;
    }
    .ai-badge {
        background: #e2d5f1;
        color: #5a2d82;
        border-radius: 5px;
        padding: 3px 10px;
        font-size: 11px;
    }
    div[data-testid="stCodeBlock"] {
        font-size: 16px !important;
    }
</style>
""", unsafe_allow_html=True)

# Navigation
page = st.sidebar.radio("", ["Nachrichten senden", "Kommentar", "Stats"], index=0)

if page == "Nachrichten senden":
    st.title("💬 Nachrichten senden")
    st.caption("Namen eingeben ODER Screenshot hochladen")

    input_tab2, input_tab1 = st.tabs(["Screenshot", "Text eingeben"])

    with input_tab1:
        text_input = st.text_area(
            "Namen einfügen",
            placeholder="Max Mueller\nAnna Schmidt\nJohn Doe\n\nOder direkt von LinkedIn kopieren...",
            height=200,
            key="names_input",
        )

        if st.button("Nachrichten laden", type="primary", use_container_width=True, key="btn_text_load"):
            if text_input.strip():
                with st.spinner("Lade Nachrichten aus CRM..."):
                    names = parse_names_from_text(text_input)
                    if not names:
                        st.warning("Keine Namen erkannt. Bitte Namen eingeben (ein Name pro Zeile).")
                    else:
                        all_leads = get_all_leads_with_messages()
                        matched, not_found = match_names_to_leads(names, all_leads)
                        st.session_state["matched"] = matched
                        st.session_state["not_found"] = not_found
                        st.session_state["names_count"] = len(names)
            else:
                st.warning("Bitte Namen einfügen.")

    with input_tab2:
        screenshot = st.file_uploader(
            "Screenshot hochladen oder reinziehen",
            type=["png", "jpg", "jpeg", "webp"],
            key="names_screenshot",
            help="Drag & Drop oder auswaehlen",
        )

        st.markdown("**oder**")
        paste_result = pbutton("Screenshot einfuegen (Ctrl+V)", key="paste_names")

        # Determine image source: uploaded file or pasted
        names_image_bytes = None
        names_media_type = "image/png"
        if screenshot:
            names_image_bytes = screenshot.getvalue()
            names_media_type = get_media_type(screenshot.name)
            st.image(screenshot, caption="Screenshot", use_container_width=True)
        elif paste_result and paste_result.image_data is not None:
            buf = io.BytesIO()
            paste_result.image_data.save(buf, format="PNG")
            names_image_bytes = buf.getvalue()
            names_media_type = "image/png"
            st.image(paste_result.image_data, caption="Eingefuegter Screenshot", use_container_width=True)

        if st.button("Namen aus Screenshot erkennen", type="primary", use_container_width=True, key="btn_screenshot_load"):
            if names_image_bytes:
                with st.spinner("Analysiere Screenshot mit AI..."):
                    image_bytes = names_image_bytes
                    media_type = names_media_type
                    analysis, error = gemini_request(
                        SCREENSHOT_PROMPT_NAMES, image_bytes, media_type
                    )
                    if error:
                        st.error(f"Fehler: {error}")
                    elif analysis:
                        names = parse_names_from_screenshot(analysis)
                        if not names:
                            st.warning("Keine Namen im Screenshot erkannt.")
                            with st.expander("AI-Analyse anzeigen"):
                                st.text(analysis)
                        else:
                            st.success(f"{len(names)} Namen erkannt: {', '.join(names)}")
                            with st.spinner("Lade Nachrichten aus CRM..."):
                                all_leads = get_all_leads_with_messages()
                                matched, not_found = match_names_to_leads(names, all_leads)
                                st.session_state["matched"] = matched
                                st.session_state["not_found"] = not_found
                                st.session_state["names_count"] = len(names)
            else:
                st.warning("Bitte erst einen Screenshot hochladen oder einfuegen (Ctrl+V).")

    # Show results
    if st.session_state.get("matched") is not None:
        matched = st.session_state["matched"]
        not_found = st.session_state.get("not_found", [])
        total = st.session_state.get("names_count", 0)

        st.markdown(f"**{len(matched)} von {total} gefunden**")

        if not_found:
            with st.expander(f"Nicht gefunden ({len(not_found)})"):
                for nf in not_found:
                    st.markdown(f'<span class="notfound-badge">{nf}</span> ', unsafe_allow_html=True)
                st.caption("Diese Namen haben noch keine Nachricht im CRM. Sag Koren Bescheid.")

        sent_count = 0
        for i, item in enumerate(matched):
            rec = item["record"]
            f = rec.get("fields", {})
            name = f.get("Name", item["name"])
            vorname = f.get("Vorname", name.split()[0] if name else "")
            firma = f.get("Firma", "")
            position = f.get("Position", "")
            msg = f.get("Personalisierte Nachricht", "")
            status = f.get("Nachricht Status", "")
            rec_id = rec["id"]

            info_parts = [p for p in [position, firma] if p]

            if status == "Gesendet":
                st.markdown(f"""<div class="message-box">
<div class="lead-name">{i+1}. {name} <span class="sent-badge">SCHON GESENDET</span></div>
<div class="lead-info">{' | '.join(info_parts)}</div>
</div>""", unsafe_allow_html=True)
                sent_count += 1
                continue

            if not msg or len(msg) < 5:
                # Auto-generate message if none exists
                gen_key = f"gen_{rec_id}"
                st.markdown(f"""<div class="message-box">
<div class="lead-name">{i+1}. {name} <span class="skip-badge">KEINE NACHRICHT</span></div>
<div class="lead-info">{' | '.join(info_parts)}</div>
</div>""", unsafe_allow_html=True)

                if st.button("Nachricht generieren", key=gen_key, use_container_width=True):
                    with st.spinner(f"Generiere + pruefe Nachricht fuer {vorname}..."):
                        generated, validation = generate_message_for_lead(f)
                        if generated:
                            # Show validation result
                            if validation and validation["status"] == "FAIL":
                                st.warning(f"Validierung: {validation['problems']}")
                                st.code(generated, language=None)
                                st.caption("Diese Nachricht hat die Pruefung NICHT bestanden. Bitte manuell pruefen!")
                            else:
                                st.success("Nachricht geprueft und OK")
                                st.code(generated, language=None)
                                try:
                                    save_message_to_airtable(rec_id, generated)
                                    st.session_state[f"ai_generated_{rec_id}"] = True
                                    for m in st.session_state["matched"]:
                                        if m["record"]["id"] == rec_id:
                                            m["record"]["fields"]["Personalisierte Nachricht"] = generated
                                            m["record"]["fields"]["Nachricht Status"] = "Entwurf"
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Speichern fehlgeschlagen: {e}")
                        else:
                            st.error("Konnte keine Nachricht generieren. Bitte nochmal versuchen.")
                continue

            # Check if message was AI-generated in this session or from CRM
            source_badge = '<span class="crm-badge">aus CRM</span>'
            if st.session_state.get(f"ai_generated_{rec_id}"):
                source_badge = '<span class="ai-badge">AI-generiert</span>'

            st.markdown(f"""<div class="message-box">
<div class="lead-name">{i+1}. {name} {source_badge}</div>
<div class="lead-info">{' | '.join(info_parts)}</div>
</div>""", unsafe_allow_html=True)

            st.code(msg, language=None)

            col1, col2 = st.columns([3, 1])
            with col1:
                st.caption(f"Kopieren → an {vorname} senden")
            with col2:
                if st.button("Gesendet ✓", key=f"sent_{rec_id}", use_container_width=True):
                    try:
                        mark_as_sent(rec_id)
                        for m in st.session_state["matched"]:
                            if m["record"]["id"] == rec_id:
                                m["record"]["fields"]["Nachricht Status"] = "Gesendet"
                        st.rerun()
                    except Exception as e:
                        st.error(f"Fehler: {e}")

            st.divider()

        if sent_count > 0:
            st.info(f"{sent_count} davon bereits gesendet")


elif page == "Kommentar":
    st.title("💬 Kommentar-Generator")
    st.caption("Screenshots hochladen oder einfuegen → Kommentare fuer alle Posts")

    # Multiple screenshot upload
    post_screenshots = st.file_uploader(
        "Screenshots reinziehen (einzeln oder mehrere)",
        type=["png", "jpg", "jpeg", "webp"],
        key="post_screenshots",
        help="Drag & Drop oder auswaehlen",
        accept_multiple_files=True,
    )

    st.markdown("**oder**")
    paste_result_comment = pbutton("Screenshot einfuegen (Ctrl+V)", key="paste_comment")

    # Combine uploaded + pasted images
    comment_images = list(post_screenshots) if post_screenshots else []
    if paste_result_comment and paste_result_comment.image_data is not None:
        # Convert PIL to file-like for uniform handling
        buf = io.BytesIO()
        paste_result_comment.image_data.save(buf, format="PNG")
        buf.seek(0)
        buf.name = "pasted_screenshot.png"
        comment_images.append(buf)
        st.image(paste_result_comment.image_data, caption="Eingefuegter Screenshot", use_container_width=True)

    if comment_images:
        for img in post_screenshots or []:
            st.image(img, caption=img.name, use_container_width=True, width=300)

        if st.button("Kommentare fuer alle generieren", type="primary", use_container_width=True, key="btn_comment_batch"):
            all_batch_results = []

            for idx, screenshot in enumerate(comment_images):
                st.markdown(f"---")
                with st.spinner(f"Analysiere Screenshot {idx+1}/{len(comment_images)}..."):
                    image_bytes = screenshot.getvalue() if hasattr(screenshot, 'getvalue') else screenshot.read()
                    media_type = get_media_type(getattr(screenshot, 'name', 'pasted.png'))

                    # Use multi-post prompt to catch feed screenshots with multiple posts
                    analysis, error = gemini_request(
                        SCREENSHOT_PROMPT_MULTI_POST, image_bytes, media_type
                    )
                    if error:
                        st.error(f"Screenshot {idx+1} Fehler: {error}")
                        continue
                    if not analysis:
                        st.warning(f"Screenshot {idx+1}: Nichts erkannt")
                        continue

                    posts = parse_multi_posts_from_screenshot(analysis)
                    if not posts:
                        # Fallback: try single post parser
                        post_info = parse_post_from_screenshot(analysis)
                        if post_info["text"]:
                            posts = [post_info]

                    if not posts:
                        st.warning(f"Screenshot {idx+1}: Kein Post erkannt")
                        with st.expander("AI-Analyse anzeigen"):
                            st.text(analysis)
                        continue

                    for post in posts:
                        pname = post["name"]
                        pheadline = post["headline"]
                        ptext = post["text"]

                        with st.spinner(f"Generiere Kommentar fuer {pname}..."):
                            record = find_lead_by_name(pname) if pname else None
                            category = classify_poster(pname or "", record, pheadline)
                            cat_label, cat_emoji = CATEGORY_LABELS.get(category, ("?", "❓"))

                            prompt = build_comment_prompt(ptext, pname or "Unbekannt", category, record)
                            raw_result, gen_error = generate_comments(prompt)

                            if gen_error or not raw_result:
                                st.error(f"Fehler bei {pname}: {gen_error}")
                                continue

                            options = parse_comment_options(raw_result)
                            all_batch_results.append({
                                "name": pname,
                                "headline": pheadline,
                                "category": category,
                                "cat_label": cat_label,
                                "cat_emoji": cat_emoji,
                                "record": record,
                                "options": options,
                                "raw": raw_result,
                            })

            if all_batch_results:
                st.session_state["batch_comments"] = all_batch_results

    # Show all comment results (batch)
    if st.session_state.get("batch_comments"):
        results = st.session_state["batch_comments"]
        st.markdown(f"### {len(results)} Posts — Kommentare bereit")

        for ridx, res in enumerate(results):
            pname = res["name"]
            cat_emoji = res["cat_emoji"]
            cat_label = res["cat_label"]
            record = res.get("record")

            crm_info = ""
            if record:
                f = record.get("fields", {})
                crm_info = f" — {f.get('Firma', '')} | {f.get('Position', '')}"

            st.markdown(f"""<div class="message-box">
<div class="lead-name">{cat_emoji} {pname} — {cat_label}{crm_info}</div>
<div class="lead-info">{res.get('headline', '')}</div>
</div>""", unsafe_allow_html=True)

            options = res["options"]
            if options:
                # Show best option (first one) prominently
                best = options[0]
                issues = validate_comment(best["comment"])
                st.code(best["comment"], language=None)
                if issues:
                    st.warning(" | ".join(issues))
                else:
                    st.success("Geprueft: OK")
                st.caption(f"Kopieren → bei {pname} als Kommentar posten")

                # Show alternatives in expander
                if len(options) > 1:
                    with st.expander(f"Alternativen fuer {pname}"):
                        for opt in options[1:]:
                            st.markdown(f"**{opt['label']} | {opt['formula']}**")
                            st.code(opt["comment"], language=None)
                            alt_issues = validate_comment(opt["comment"])
                            if alt_issues:
                                st.warning(" | ".join(alt_issues))
                            else:
                                st.success("OK")
            else:
                st.code(res.get("raw", ""), language=None)

            st.divider()

        if st.button("Neue Kommentare", use_container_width=True):
            st.session_state["batch_comments"] = None
            st.rerun()


elif page == "Stats":
    st.title("📊 Stats")

    if st.button("Stats laden", type="primary", use_container_width=True):
        with st.spinner("Lade..."):
            stats = get_stats()

        col1, col2 = st.columns(2)
        col1.metric("Gesamt Leads", stats["Gesamt"])
        col2.metric("Offen (Entwurf)", stats["Entwurf"])

        col3, col4 = st.columns(2)
        col3.metric("Gesendet", stats["Gesendet"])
        col4.metric("Beantwortet", stats["Beantwortet"])

        if stats["Gesamt"] > 0:
            done = stats["Gesendet"] + stats["Beantwortet"]
            pct = round(done / stats["Gesamt"] * 100)
            st.progress(pct / 100, text=f"{pct}% verarbeitet ({done}/{stats['Gesamt']})")
