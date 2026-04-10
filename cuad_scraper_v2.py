"""
CUAD Member Club Instagram Scraper v2 (Playwright)
====================================================
Uses a real browser (Chromium) instead of Instaloader's broken API calls.
Scrapes post date, likes, comment count, and caption for each club account.

Output: cuad_instagram_data.csv

Setup:
    pip install playwright
    python -m playwright install chromium

Usage:
    python cuad_scraper_v2.py                          # prompts for login
    python cuad_scraper_v2.py --since 2023-10-01
    python cuad_scraper_v2.py --since 2023-10-01 --until 2025-06-01
    python cuad_scraper_v2.py --max-posts 30           # quick test run
    python cuad_scraper_v2.py --headless               # no browser window

Session:
    After first login a session file (ig_browser_session/) is saved.
    Subsequent runs reuse it — you won't need to log in again unless
    Instagram invalidates the session.

Rate limiting:
    The script waits between posts and between accounts. Don't remove sleeps.
    If you get a checkpoint/captcha page, run without --headless so you can
    solve it manually. The session will be saved afterward.
"""

import csv
import json
import time
import random
import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ─────────────────────────────────────────────────────────────────
# CLUB LIST  —  (display_name, handle, known_status)
#
# known_status values:
#   None             = normal, attempt scrape
#   "banned"         = confirmed banned by Meta; log but don't scrape
#   "private"        = confirmed private; skip scraping
#   "no_handle"      = no Instagram account found
#   "maybe_removed"  = handle may have been removed; attempt but expect 404
# ─────────────────────────────────────────────────────────────────
CLUBS = [
    # CUAD coalition account (not in the op-ed list but central to study)
    #("Columbia University Apartheid Divest (CUAD)",                    "cuapartheiddivest",         "banned"),

    # ── All 95 CUAD member orgs — Nov 14 2023 Spectator op-ed ────
    #("Students for Justice in Palestine",                              "sjp.columbia",              "banned"),
    #("Jewish Voice for Peace",                                         "columbia.jvp",              None),
    #("Sunrise Columbia",                                               "sunrisecolumbia",           None),
    #("Somali Student Association",                                     "columbia.ssa",              None),
    #("Center for the Study of Ethnicity and Race Student Adv. Board", "csersabcolumbia",           None),
    #("Young Democratic Socialists of America",                         "columbiaydsa",              None),
    #("Columbia Queer and Asian",                                       "columbia_qna",              None),
    #("Asian American Alliance",                                        "columbia.aaa",              None),
    #("Columbia Queer Alliance",                                        "columbiaqueeralliance",     None),
    #("African Students Association",                                   "asacolumbia",               None),
    #("Barnard Columbia Abolitionist Collective",                       "bcabolitioncollective",     None),
    #("Housing Equity Project",                                         "columbia_hep",              None),
    #("AAPI Interboard",                                                "cu_aapi",                   None),
    #("White Coats 4 Black Lives",                                      "columbia_wc4bl",            None),
    #("Global Learning Exchange",                                       "glecolumbia",               None),
    #("CU Afghan Student Alliance",                                     "columbia.afghans",          None),
    #("Graduate Muslim Student Association",                            "columbiagmsa",              None),
    #("Columbia Social Workers for Palestine",                          "cssw4palestine",            None),
    #("Poetry Slam",                                                    "cuslampoetry",              None),
    #("Proud Colors",                                                   "cuproudcolors",             None),
    #("Student Worker Solidarity",                                      "swscolumbia",               None),
    #("Law School Coalition for a Free Palestine",                      "clsforfreepalestine",       None),
    #("Student Workers of Columbia",                                    "sw_columbia",               None),
    #("Black Student Organization",                                     "columbiabso",               None),
    #("SIPA Palestine Working Group",                                   "sipapwg",                   None),
    #("Columbia Vietnamese Students Association",                       "columbia.vsa",              None),
    #("Columbia Law Students for Palestine",                            "clsforpalestine",           None),
    #("Dar: the Palestine Student Union",                               None,                        "no_handle"),
    #("Columbia National Lawyers Guild",                                "cls_nlg",                   None),
    #("Muslim Students Association",                                    "columbia_msa",              None),
    #("African Studies Working Group",                                  "tc_aswg1",                  None),
    #("Caribbean Students Association",                                 "csacolumbia",               None),
    ("Barnard Organization of Soul and Solidarity",                    "barnardboss",               None),
    ("AZINE Asian / American Arts & Zine Collective",                  None,          "azine.collective private"),
    ("CU Turath (Arab Students Association)",                          "cuturath",                  None),
    ("Columbia Humanitarian Org. for Migration and Emergencies",       None,                        "no_handle"),
    ("Reproductive Justice Collective",                                "reprojusticecollective",    None),
    ("Columbia University Black Pre-Professional Society",             "cubps",                     None),
    ("Pakistani Students Association",                                 "columbia.psa",              None),
    ("Barnard Columbia Urban Review",                                  "bc.urbanreview",            None),
    ("Sabor",                                                          "cusabor",                   None),
    ("Masaha",                                                         "masaha_gsapp",              None),
    ("Club Bangla",                                                    None,                  "cubangla private"),
    ("Mixed Heritage Society",                                         "mhscolumbia",               None),
    ("Columbia Chicanx Caucus",                                        None,                        "no_handle"),
    ("VP&S Black and Latinx Student Organization",                     None,                        "no_handle"),
    ("Columbia Middle Eastern Law Association",                        None,                        "no_handle"),
    ("RightsViews (Human Rights Graduate Journal)",                    "rightsviews_columbia",      None),
    ("School of Social Work Abolition Caucus",                         "abolitioncssw",             None),
    ("Hifi Snock Uptown",                                              "hifisnockuptown",           None),
    ("Take Back The Night",                                            "cu_tbtn",                   None),
    ("Native American Council",                                        "nativeamericancouncil",     None),
    ("VP&S Global Health Organization",                                "Columbia.GHO",              None),
    ("VP&S Equity and Justice Fellowship",                             None,                        "no_handle"),
    ("Columbia Law Restorative Justice Collective",                    "columbiacenterforjustice",  None),
    ("Mujeres",                                                        "barnardmurjeres",           None),
    ("The Columbia Review",                                            "thecolumbiareview",         None),
    ("Student Organization of Latines",                                "cu_sol",                    None),
    ("Alianza",                                                        "cu.alianza",                None),
    ("GSAS Queer Graduate Collective",                                 None,       "queergradcollective private"),
    ("CU Amnesty International",                                       "amnestycolumbia",           None),
    ("Columbia South Asian Feminisms Alliance",                        "cu_safa",                   None),
    ("Union Theological Seminary Students for a Free Palestine",       "uts.slp",                   None),
    ("Muslim Law Students Association",                                "columbia_mlsa",             None),
    ("Columbia Law Parole Advocacy Project",                           "pap_columbia",              None),
    ("Mariachi Leones de Columbia",                                    "mariachileonesdecolumbia",  None),
    ("Columbia Asian Pacific American Medical Student Association",    "cuapamsa",                  None),
    ("Columbia Law and Political Economy",                             "cls_lpe",                   None),
    ("Columbia Care Access Project",                                   None,                        "no_handle"),
    ("Columbia University Asian Pacific American Heritage Month",      "cuapahm",                   None),
    ("Native American Law Students Association",                       "columbia_nalsa",            None),
    ("Columbia University Students for Human Rights",                  "cuhumanrights",             None),
    ("Raw Elementz",                                                   "raw_elementz",              None),
    ("WBAR Radio",                                                     "wbar_radio",                None),
    ("South Asian Law Students Association",                           "columbiasalsa",             None),
    ("Latinx Law Students Association",                                "lalsa.cls",                 None),
    ("Columbia Law School Empowering Women of Color",                  "ewoc_columbia",             None),
    ("Black Law Students Association at Columbia Law",                 "columbia_blsa",             None),
    ("Sexual and Reproductive Health Action Group (Mailman)",          "shag.columbia.mph",         None),
    ("CSSW Queer Caucus",                                              "queercaucusatcssw",         None),
    ("CURA Collective",                                                "cu_racollective",           None),
    ("Students for Sanctuary",                                         "studentsforsanctuary",      None),
    ("Journal for Criminal Justice",                                   "columlrev",                 None),
    ("CLS Human Rights Association",                                   None,                       "clshumanrights private"),
    ("Columbia's New York Small Claims Advisory Service",              None,                        "no_handle"),
    ("Students for Free Tibet",                                        "sft_columbia",              None),
    ("Payments for Placements Caucus (CSSW)",                          "p4p_cssw",                  None),
    ("Columbia Policy Institute",                                      "columbiapolicyinstitute",   None),
    ("4x4 Magazine",                                                   "fourbyfourmag",             None),
    ("Genderev",                                                       "cugenderev",                None),
    ("Anthropology Graduate Student Association",                      "tcanthro",                  None),
    ("Human Rights Solidarity Network",                                None,                        "no_handle"),
    ("CLS Society for Immigrant and Refugee Rights",                   "cls.sirr",                  None),
    ("Columbia University Alumni For Palestine",                       "cualumni4palestine",        None),
    ("Jafra",                                                          "cujafra",                   None),
]

SESSION_DIR = "ig_browser_session"
IG_LOGIN_URL = "https://www.instagram.com/accounts/login/"
IG_PROFILE_URL = "https://www.instagram.com/{}/"


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def jitter(base: float) -> None:
    """Sleep for base ± 30% to avoid rhythmic request patterns."""
    time.sleep(base * (0.7 + random.random() * 0.6))


def parse_args():
    p = argparse.ArgumentParser(description="Scrape CUAD club Instagram data (Playwright)")
    p.add_argument("--max-posts", type=int, default=None)
    p.add_argument("--since",     type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--until",     type=str, default=None, help="YYYY-MM-DD")
    p.add_argument("--output",    type=str, default="cuad_instagram_data_2.csv")
    p.add_argument("--sleep",     type=float, default=3.0,
                   help="Base sleep between posts in seconds (default: 3)")
    p.add_argument("--headless",  action="store_true",
                   help="Run without a visible browser window")
    p.add_argument("--restart",   action="store_true",
                   help="Ignore existing output file and start from scratch")
    return p.parse_args()


# ─────────────────────────────────────────────────────────────────
# LOGIN / SESSION
# ─────────────────────────────────────────────────────────────────

def login_and_save_session(playwright, headless: bool) -> list:
    """Open a browser, log in interactively, return storage state."""
    print("\n[!] No saved session found. Opening browser for login...")
    print("    Log in to Instagram, then come back here and press Enter.\n")

    browser = playwright.chromium.launch(headless=False)  # always visible for login
    ctx = browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    page = ctx.new_page()
    page.goto(IG_LOGIN_URL, wait_until="networkidle")

    # Pre-fill credentials from env if available
    username = os.environ.get("IG_USERNAME", "")
    password = os.environ.get("IG_PASSWORD", "")
    if username:
        page.fill('input[name="username"]', username)
    if password:
        page.fill('input[name="password"]', password)
        page.click('button[type="submit"]')

    input("    >>> Browser is open. Log in (solve any 2FA/captcha), then press Enter here: ")

    state = ctx.storage_state()
    Path(SESSION_DIR).mkdir(exist_ok=True)
    with open(f"{SESSION_DIR}/state.json", "w") as f:
        json.dump(state, f)

    print(f"[+] Session saved to {SESSION_DIR}/state.json")
    browser.close()
    return state


def load_or_create_session(playwright, headless: bool):
    """Return (browser, context) using saved session or fresh login."""
    state_path = f"{SESSION_DIR}/state.json"

    if os.path.exists(state_path):
        print(f"[+] Loading saved session from {state_path}")
        with open(state_path) as f:
            state = json.load(f)
    else:
        state = login_and_save_session(playwright, headless)

    browser = playwright.chromium.launch(headless=headless)
    ctx = browser.new_context(
        storage_state=state,
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    )
    return browser, ctx


# ─────────────────────────────────────────────────────────────────
# SCRAPING
# ─────────────────────────────────────────────────────────────────

def check_for_checkpoint(page) -> bool:
    """Return True if Instagram is showing a challenge/checkpoint page."""
    url = page.url
    return any(x in url for x in ["challenge", "checkpoint", "login", "accounts/login"])


def scrape_post_data(page, post_url: str) -> dict:
    """
    Extract likes, comments, caption, and date from a post page.

    Instagram renders post stats as an unlabeled newline-separated block:
        "{likes}\n{comments}\n{shares}\n{Month Day}"
    We parse that directly. Caption is found in article text nodes.
    """
    import re as _re

    result = {"likes": None, "comments": None, "reposts": None, "caption": None,
              "post_date": None, "post_timestamp": None}

    try:
        # "commit" fires on first response byte — doesn't throw on 4xx/5xx,
        # so we can inspect the status code instead of catching an exception.
        response = page.goto(post_url, wait_until="commit", timeout=20000)
        if response and response.status == 404:
            result["_deleted"] = True
            return result
        if response and response.status >= 400:
            result["_rate_limited"] = True
            return result
        # Page returned 2xx — wait for content to render
        try:
            page.wait_for_selector("time[datetime]", timeout=10000)
        except Exception:
            pass
        jitter(2)
    except PWTimeout:
        return result
    except Exception as e:
        if "ERR_HTTP_RESPONSE_CODE_FAILURE" in str(e):
            # Chromium throws this for 4xx before we can read response.status.
            # Treat as rate-limited (not deleted) so backoff logic triggers.
            result["_rate_limited"] = True
            return result
        print(f"    [post error] {e}")
        return result

    if check_for_checkpoint(page):
        print("    [!] Checkpoint detected — re-login may be needed")
        return result

    # ── Date ─────────────────────────────────────────────────────
    try:
        time_el = page.query_selector("time[datetime]")
        if time_el:
            dt_str = time_el.get_attribute("datetime")
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
            result["post_date"] = dt.strftime("%Y-%m-%d")
            result["post_timestamp"] = dt.isoformat()
    except Exception:
        pass

    # ── Likes & comments ─────────────────────────────────────────
    # Instagram renders stats as: "{N}\n{N}\n{N}\n{Month Day}"
    # First number = likes, second = comments
    try:
        all_texts = page.evaluate("""() => {
            const texts = [];
            document.querySelectorAll('*').forEach(el => {
                if (el.children.length < 8) {
                    const t = (el.innerText || '').trim();
                    if (t) texts.push(t);
                }
            });
            return texts;
        }""")

        # Number token: digits/commas, optionally followed by K or M abbreviation
        # e.g. "44", "8K", "1.2M", "1,234"
        NUM = r'[\d][\d,\.]*[KkMm]?'

        # Stats block patterns we've seen:
        #   likes\ncomments\nreposts\ndate  →  3 numbers
        #   likes\ncomments\ndate          →  2 numbers (no reposts)
        #   likes\ndate                    →  1 number  (no comments/reposts)
        # All numbers can be abbreviated (8K, 1.2M).
        # Date suffix is anything after the last number — we don't validate it.
        DATE = (r'(?:\d+[wdhmy]'
                r'|(?:January|February|March|April|May|June|July|August|'
                r'September|October|November|December)\s+\d[\d,]*(?:,\s*\d{4})?)')

        stats_pattern = _re.compile(
            r'^(' + NUM + r')(?:\n(' + NUM + r')(?:\n(' + NUM + r'))?)?\n(' + DATE + r')$',
            _re.IGNORECASE
        )

        def parse_count(s):
            """Parse '8K' → 8000, '1.2M' → 1200000, '1,234' → 1234."""
            if not s:
                return None
            s = s.strip().replace(',', '')
            try:
                if s[-1] in ('K', 'k'):
                    return int(float(s[:-1]) * 1_000)
                elif s[-1] in ('M', 'm'):
                    return int(float(s[:-1]) * 1_000_000)
                return int(float(s))
            except (ValueError, IndexError):
                return None

        # Also handle labeled text ("1,234 likes") as a fallback
        likes_label   = _re.compile(r'^(' + NUM + r')\s+likes?$', _re.IGNORECASE)
        comment_label = _re.compile(r'(' + NUM + r')\s+comments?', _re.IGNORECASE)

        for text in all_texts:
            m = stats_pattern.match(text)
            if m:
                result["likes"]    = parse_count(m.group(1))
                result["comments"] = parse_count(m.group(2)) if m.group(2) else 0
                result["reposts"]  = parse_count(m.group(3)) if m.group(3) else 0
                break
            if result["likes"] is None:
                m2 = likes_label.match(text)
                if m2:
                    result["likes"] = parse_count(m2.group(1))
            if result["comments"] is None:
                m3 = comment_label.search(text)
                if m3:
                    result["comments"] = parse_count(m3.group(1))
    except Exception:
        pass

    # ── Caption ──────────────────────────────────────────────────
    # Look for the longest text node inside the article that isn't
    # a UI label — captions are typically the longest text on the page
    try:
        caption = page.evaluate("""() => {
            const SKIP = new Set([
                'Follow', 'Unfollow', 'Like', 'Comment', 'Share', 'Save',
                'More', 'Reply', 'View replies', 'Hide replies', 'Instagram',
                'Home', 'Reels', 'Messages', 'Search', 'Explore',
                'Notifications', 'Create', 'Profile', 'Also from Meta',
            ]);
            const article = document.querySelector('article');
            const root = article || document.body;

            let best = '';
            root.querySelectorAll('span, div, p, h1, h2').forEach(el => {
                // Skip elements with many children (layout containers)
                if (el.children.length > 3) return;
                const t = (el.innerText || '').trim();
                // Caption is longer than UI labels and not a known UI string
                if (t.length > 10 && t.length > best.length && !SKIP.has(t)
                        && !t.startsWith('{') && !t.startsWith('http')) {
                    best = t;
                }
            });
            return best || null;
        }""")
        if caption:
            result["caption"] = caption[:500].replace("\n", " ")
    except Exception:
        pass

    return result


def get_post_links_from_profile(page, handle: str, max_posts: int = None,
                                since_dt=None, until_dt=None) -> list[str]:
    """
    Collect post/reel URLs by calling Instagram's internal API directly
    from within the logged-in browser context (so session cookies are sent).

    Flow:
      1. Load the profile page (establishes cookies, checks 404/private).
      2. Call web_profile_info to get the numeric user_id.
      3. Paginate /api/v1/feed/user/{user_id}/ until more_available=false.
    """
    import re, json as _json

    # ── 1. Load profile page ──────────────────────────────────────
    page.goto(IG_PROFILE_URL.format(handle), wait_until="domcontentloaded", timeout=20000)
    jitter(2)

    if check_for_checkpoint(page):
        print(f"    [!] Session challenge on @{handle}")
        return []

    try:
        rendered_text = page.inner_text("body")
    except Exception:
        rendered_text = ""

    if (page.url.rstrip("/") in ("https://www.instagram.com", "https://www.instagram.com/")
            or "Sorry, this page isn't available" in rendered_text):
        return None

    if "This Account is Private" in rendered_text:
        return []

    # ── 2. Get user_id via web_profile_info (called from page so cookies go with it)
    user_id = None
    try:
        profile_data = page.evaluate(r"""async (handle) => {
            const r = await fetch(
                "https://i.instagram.com/api/v1/users/web_profile_info/?username=" + handle,
                {
                    credentials: "include",
                    headers: {
                        "X-IG-App-ID": "936619743392459",
                        "Accept": "application/json"
                    }
                }
            );
            if (!r.ok) return null;
            return await r.json();
        }""", handle)

        if profile_data:
            user = ((profile_data.get("data") or {}).get("user") or
                    (profile_data.get("graphql") or {}).get("user") or
                    profile_data.get("user") or {})
            user_id = str(user.get("id") or user.get("pk") or "")
    except Exception as e:
        print(f"    [!] web_profile_info failed: {e}")

    # Fallback: extract user_id from page HTML
    if not user_id:
        try:
            html = page.content()
            for pattern in [r'"pk"\s*:\s*"(\d{5,})"', r'"id"\s*:\s*"(\d{5,})"',
                             r'profilePage_(\d{5,})', r'"owner":\{"id":"(\d+)"']:
                m = re.search(pattern, html)
                if m:
                    user_id = m.group(1)
                    break
        except Exception:
            pass

    if not user_id:
        print(f"    [!] Could not find user_id for @{handle}, falling back to DOM scroll")

    # ── 3. Paginate feed API ──────────────────────────────────────
    post_links = set()

    def extract_from_items(items):
        for item in (items or []):
            code = item.get("code") or item.get("shortcode")
            if not code:
                # sometimes nested under media key
                code = (item.get("media") or {}).get("code")
            if code:
                if item.get("media_type") == 2:
                    post_links.add(f"https://www.instagram.com/reel/{code}/")
                else:
                    post_links.add(f"https://www.instagram.com/p/{code}/")

    if user_id:
        cursor = None
        page_num = 0
        while True:
            if max_posts and len(post_links) >= max_posts:
                break

            qs = f"count=12{'&max_id=' + cursor if cursor else ''}"
            try:
                data = page.evaluate(r"""async ({userId, qs}) => {
                    const r = await fetch(
                        "https://www.instagram.com/api/v1/feed/user/" + userId + "/?" + qs,
                        {
                            credentials: "include",
                            headers: {
                                "X-IG-App-ID": "936619743392459",
                                "Accept": "application/json"
                            }
                        }
                    );
                    if (!r.ok) return null;
                    return await r.json();
                }""", {"userId": user_id, "qs": qs})
            except Exception as e:
                print(f"    [!] Feed API error: {e}")
                break

            if not data:
                print(f"    [!] Feed API returned null (session may have expired)")
                break

            extract_from_items(data.get("items") or [])
            cursor = data.get("next_max_id")
            more   = data.get("more_available", False)
            page_num += 1
            print(f"    ...page {page_num}, {len(post_links)} posts total")

            if not more or not cursor:
                break

            jitter(2)

    # ── DOM scroll fallback ───────────────────────────────────────
    if not post_links:
        print(f"    [!] API yielded nothing for @{handle}, falling back to DOM scroll")
        own_p    = re.compile(r'^/' + re.escape(handle.lower()) + r'/p/[^/]+/?$',    re.IGNORECASE)
        own_r    = re.compile(r'^/' + re.escape(handle.lower()) + r'/reel/[^/]+/?$', re.IGNORECASE)
        bare     = re.compile(r'^/(?:p|reel)/[^/]+/?$')
        for i in range(60):
            for a in page.query_selector_all('a[href*="/p/"], a[href*="/reel/"]'):
                href = (a.get_attribute("href") or "").split("?")[0].rstrip("/") + "/"
                if own_p.match(href) or own_r.match(href) or bare.match(href):
                    post_links.add("https://www.instagram.com" + href)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            jitter(2)

    links = list(post_links)
    if max_posts:
        links = links[:max_posts]
    return links


def scrape_club(page, display_name: str, handle: str, known_status: str,
                since_dt, until_dt, max_posts: int, sleep_secs: float,
                completed_urls: set = None, csv_writer=None) -> list:
    rows = []
    if completed_urls is None:
        completed_urls = set()

    def _write(row):
        """Append row to in-memory list and flush to CSV immediately."""
        rows.append(row)
        if csv_writer:
            csv_writer.writerow(row)

    # Short-circuit cases where we already know the outcome
    if known_status == "no_handle" or not handle:
        print(f"  [NO HANDLE] {display_name}")
        _write(_error_row(display_name, handle, "no_handle"))
        return rows

    if known_status == "private":
        print(f"  [PRIVATE] @{handle} ({display_name}) — known private, skipping")
        _write(_error_row(display_name, handle, "private_known"))
        return rows

    if known_status == "banned":
        print(f"  [BANNED] @{handle} ({display_name}) — confirmed Meta ban, logging without scraping")
        _write(_error_row(display_name, handle, "banned_by_meta"))
        return rows

    print(f"  Scraping @{handle} ({display_name})...")

    try:
        post_links = get_post_links_from_profile(page, handle, max_posts, since_dt, until_dt)
    except Exception as e:
        print(f"  [ERROR] Failed to load profile @{handle}: {e}")
        _write(_error_row(display_name, handle, f"profile_error: {e}"))
        return rows

    if post_links is None:
        print(f"  [NOT FOUND] @{handle} — deleted, banned, or username wrong")
        _write(_error_row(display_name, handle, "not_found"))
        return rows

    if post_links == []:
        print(f"  [PRIVATE/EMPTY] @{handle}")
        _write(_error_row(display_name, handle, "private_or_empty"))
        return rows

    # Filter out posts already collected in a previous (interrupted) run
    new_links = [u for u in post_links if u not in completed_urls]
    skipped = len(post_links) - len(new_links)
    if skipped:
        print(f"    Skipping {skipped} already-collected posts, {len(new_links)} remaining...")
    print(f"    Found {len(new_links)} post links to scrape...")

    if not new_links:
        print(f"  → All posts already collected for @{handle}")
        return rows

    # After heavy feed pagination Instagram often rate-limits post requests.
    if len(post_links) > 50:
        cooldown = 15 + random.uniform(0, 10)
        print(f"    [cooldown] Large account — waiting {cooldown:.0f}s before scraping posts...")
        time.sleep(cooldown)

    # Probe the first post before committing to scraping the whole list.
    # If it's rate-limited, the session is exhausted — stop this account now.
    probe = scrape_post_data(page, new_links[0])
    if probe.get("_rate_limited"):
        print(f"    [!] Session rate-limited — skipping @{handle}, will resume next run")
        return rows
    if not probe.get("_deleted"):
        # Probe succeeded — process its data normally below in the main loop
        # by putting it back; we'll re-scrape it (cheap, already loaded).
        pass  # handled by including new_links[0] in the loop as usual

    consecutive_ratelimit = 0
    backoff_count = 0
    MAX_CONSECUTIVE = 5   # trigger a backoff after this many rate-limit hits in a row
    MAX_BACKOFFS    = 3   # give up on this account after this many backoffs

    for i, post_url in enumerate(new_links):
        data = scrape_post_data(page, post_url)

        if data.pop("_deleted", False):
            # Post is gone — skip silently, don't count against rate-limit quota
            completed_urls.add(post_url)  # don't retry on next resume
            continue

        if data.pop("_rate_limited", False):
            consecutive_ratelimit += 1
            if consecutive_ratelimit >= MAX_CONSECUTIVE:
                backoff_count += 1
                wait_secs = 60 * backoff_count + random.uniform(0, 30)
                print(f"    [!] {consecutive_ratelimit} consecutive rate-limits "
                      f"(backoff #{backoff_count}/{MAX_BACKOFFS}) — sleeping {wait_secs:.0f}s...")
                if backoff_count >= MAX_BACKOFFS:
                    print(f"    [!] Max backoffs reached — skipping remaining posts for @{handle}")
                    break
                time.sleep(wait_secs)
                page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
                if check_for_checkpoint(page):
                    print("    [!] Checkpoint detected — stopping post scrape for this account")
                    break
                consecutive_ratelimit = 0
            continue
        else:
            consecutive_ratelimit = 0

        # Date filtering (skip if outside window)
        if data["post_timestamp"]:
            post_dt = datetime.fromisoformat(data["post_timestamp"])
            if until_dt and post_dt > until_dt:
                completed_urls.add(post_url)
                jitter(sleep_secs)
                continue
            if since_dt and post_dt < since_dt:
                # Posts are not strictly chronological from scroll, so don't break
                completed_urls.add(post_url)
                jitter(sleep_secs)
                continue

        collaborators, clean_caption = parse_caption(data.get("caption"))
        collab_count = len([c for c in collaborators.split(",") if c.strip()]) if collaborators else 0
        _write({
            "club": display_name,
            "handle": handle,
            "status": "ok",
            **data,
            "collaborators": collaborators,
            "collab_count": collab_count,
            "caption": clean_caption,
            "severity": grade_caption(clean_caption),
            "post_url": post_url,
        })
        completed_urls.add(post_url)

        if (i + 1) % 5 == 0:
            print(f"    ...{i+1}/{len(new_links)} posts done")

        # Random sleep: base interval plus occasional longer "human" pause
        # Every ~20 posts take a longer break to reduce rate-limit risk
        if (i + 1) % 20 == 0:
            long_pause = sleep_secs * 4 + random.uniform(5, 20)
            print(f"    [pause] Taking a {long_pause:.0f}s break at post {i+1}...")
            time.sleep(long_pause)
        else:
            # Wide jitter: base ± 50%, so at default 3s → 1.5–4.5s
            time.sleep(sleep_secs * (0.5 + random.random()))

    print(f"  → {len(rows)} posts collected for @{handle}")
    return rows


def parse_caption(raw: str) -> tuple[str, str]:
    """
    Instagram captions scraped from post pages include a header block:
        "{collab1} {collab2} ... and N others {poster}   {time}\\n{actual caption}"

    This function splits on the time marker (e.g. "3w", "2d", "1y") and returns:
        (collaborators, clean_caption)

    Collaborators is a comma-separated string of Instagram handles extracted
    from the header. clean_caption is the actual post text.

    If no time marker is found, returns ("", raw) unchanged.
    """
    import re as _re
    if not raw:
        return ("", raw or "")

    # Time markers: 3w, 96w, 2d, 14h, 5m, 1y — always a number + single letter
    time_pattern = _re.compile(r'\b(\d+[wdhmy])\b', _re.IGNORECASE)
    m = time_pattern.search(raw)

    if not m:
        return ("", raw)

    header = raw[:m.start()].strip()
    clean  = raw[m.end():].strip()

    # Extract Instagram-like handles from the header
    # (alphanumeric + dots + underscores, at least 2 chars, no pure numbers)
    handle_pattern = _re.compile(r'\b([a-zA-Z][a-zA-Z0-9_.]{1,})\b')
    SKIP = {"and", "others", "Follow", "Unfollow"}
    handles = [
        h for h in handle_pattern.findall(header)
        if h not in SKIP and not h.isdigit()
    ]
    # Deduplicate while preserving order
    seen, unique = set(), []
    for h in handles:
        if h.lower() not in seen:
            seen.add(h.lower())
            unique.append(h)

    collaborators = ", ".join(unique) if unique else ""
    return (collaborators, clean)


def grade_caption(caption: str) -> int:
    """
    Score a post caption by topic severity (highest match wins):
      5 — Palestine / Israel / Gaza
      4 — Disciplinary / expulsion
      3 — Protest / autonomous action
      2 — CUAD / SJP / anti-zionism / anti-zionist
      1 — Shafik / Armstrong / Shipman / Rosenbury
      0 — none of the above
    """
    if not caption:
        return 0
    text = caption.lower()
    TIERS = [
        (5, ["palestine", "israel", "gaza"]),
        (4, ["disciplinary", "expulsion", "expelled", "suspend", "suspended", "suspension"]),
        (3, ["protest", "autonomous action", "encampment", "demonstration", "walkout", "rally"]),
        (2, ["cuad", "sjp", "anti-zionism", "anti-zionist", "antizionism", "antizionist"]),
        (1, ["shafik", "armstrong", "shipman", "rosenbury"]),
    ]
    for score, keywords in TIERS:
        if any(kw in text for kw in keywords):
            return score
    return 0


def _error_row(display_name, handle, status):
    return {"club": display_name, "handle": handle, "status": status,
            "post_date": None, "post_timestamp": None, "likes": None,
            "comments": None, "reposts": None, "collaborators": None, "collab_count": None,
            "caption": None, "severity": None, "post_url": None}


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def main():
    args = parse_args()

    since_dt = (datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if args.since else None)
    until_dt = (datetime.strptime(args.until, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if args.until else None)

    # Deduplicate club list
    seen, clubs_deduped = set(), []
    for name, handle, known_status in CLUBS:
        key = handle or name
        if key not in seen:
            seen.add(key)
            clubs_deduped.append((name, handle, known_status))

    fieldnames = ["club", "handle", "status", "post_date", "post_timestamp",
                  "likes", "comments", "reposts", "collaborators", "collab_count", "caption",
                  "severity", "post_url"]

    # ── Resume logic ───────────────────────────────────────────────
    # Tracks both fully-done accounts AND individual post URLs already scraped,
    # so a mid-club Ctrl+C can be resumed without re-scraping those posts.
    # Use --restart to ignore all saved progress and start fresh.
    all_rows = []
    completed_handles = set()   # handles where scraping finished (any status row)
    completed_urls    = set()   # individual post URLs already in the CSV

    if not args.restart and os.path.exists(args.output):
        with open(args.output, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_rows.append(row)
                url = row.get("post_url")
                if url:
                    completed_urls.add(url)
                # A handle counts as "fully done" only if the row is an
                # error/status row (no post_url) OR we see its sentinel row.
                # We track all handles seen; the remaining-clubs filter below
                # will re-enter any club that still has unseen post links.
                if row.get("handle") and not url:
                    completed_handles.add(row["handle"])
        n_done = len(completed_handles)
        n_urls = len(completed_urls)
        if n_done or n_urls:
            print(f"[↩] Resuming — {n_done} accounts fully done, "
                  f"{n_urls} post URLs already collected.")
            print(f"    (Run with --restart to ignore saved progress and start over.)\n")
    elif args.restart and os.path.exists(args.output):
        print("[↩] --restart flag set — ignoring existing progress, starting fresh.\n")
    # ──────────────────────────────────────────────────────────────

    # Write CSV header once (fresh run or restart). On resume the file already has it.
    if args.restart or not os.path.exists(args.output):
        with open(args.output, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()

    with sync_playwright() as pw:
        browser, ctx = load_or_create_session(pw, args.headless)
        page = ctx.new_page()

        # Quick sanity check — are we actually logged in?
        page.goto("https://www.instagram.com/", wait_until="domcontentloaded")
        jitter(2)
        if check_for_checkpoint(page):
            print("\n[!] Session appears expired. Delete ig_browser_session/ and rerun.")
            browser.close()
            sys.exit(1)

        # Skip only accounts that are fully done (error/status rows, no post URLs).
        # Accounts with partial post data will be re-entered so missing posts get scraped.
        remaining = [(n, h, s) for n, h, s in clubs_deduped
                     if (h or n) not in completed_handles]
        print(f"\n[✓] Session active. Scraping {len(remaining)} accounts "
              f"({len(clubs_deduped) - len(remaining)} already complete)...\n")

        for display_name, handle, known_status in remaining:
            # Open in append mode so each row is written immediately —
            # a Ctrl+C mid-club won't lose already-scraped posts.
            with open(args.output, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                rows = scrape_club(page, display_name, handle, known_status,
                                   since_dt, until_dt, args.max_posts, args.sleep,
                                   completed_urls, csv_writer=writer)
            all_rows.extend(rows)

            # Pause between accounts. Scale with how many posts were just scraped
            # so large accounts (300+ posts) get a longer cooldown.
            if known_status not in ("banned", "private", "no_handle"):
                n_scraped = len(rows)
                if n_scraped > 100:
                    pause = 60 + random.uniform(0, 30)
                elif n_scraped > 30:
                    pause = 30 + random.uniform(0, 15)
                else:
                    pause = args.sleep * 4 + random.uniform(0, 5)
                print(f"  [between accounts] Waiting {pause:.0f}s...")
                time.sleep(pause)

        browser.close()

    print(f"\n[✓] Done. {len(all_rows)} rows written to {args.output}")

    no_handle = [(n, h) for n, h, s in CLUBS if s == "no_handle"]
    if no_handle:
        print(f"\n[!] {len(no_handle)} clubs with no Instagram handle found:")
        for name, _ in no_handle:
            print(f"    - {name}")


if __name__ == "__main__":
    main()
