"""
airbnb_strategy_pipeline.py
============================
Full 10-stage Airbnb Revenue Strategy Pipeline
Stages: A(input) → B(listing) → C(geo) → D(candidates) → E(similarity)
      → F(pricing) → G(monthly) → H(sample days) → I(synthesis) → J(output)

Usage:
    python airbnb_strategy_pipeline.py
"""

import re
import json
import math
import time
import random
import base64
import logging
import datetime
import ssl
import urllib.request
import urllib.error
import unicodedata
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
from enum import Enum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════
# ENUMS & CONSTANTS
# ══════════════════════════════════════════════════════════════

class StrategyGoal(Enum):
    MAXIMIZE_REVENUE    = "revenue"
    MAXIMIZE_OCCUPANCY  = "occupancy"
    BALANCED            = "balanced"

class SimilarityLevel(Enum):
    STRICT  = "strict"    # radius 1-2km, tight filters
    MEDIUM  = "medium"    # radius 3km
    BROAD   = "broad"     # radius 5km

class CompTier(str, Enum):
    DIRECT    = "direct"     # very similar, primary pricing reference
    SECONDARY = "secondary"  # somewhat similar, boundary check
    CONTEXT   = "context"    # same area, different type, market sentiment

class Confidence(Enum):
    HIGH   = "high"
    MEDIUM = "medium"
    LOW    = "low"

# GraphQL API constants from network log
AIRBNB_API_KEY       = "d306zoyjsyarp7ifhu67rjxn52tv0t20"
STAYS_SEARCH_HASH    = "48738417b534264d0467b107152dbe756ecb4b548418b180b2b431556245c28f"
GRAPHQL_URL          = f"https://www.airbnb.com/api/v3/StaysSearch/{STAYS_SEARCH_HASH}"

TREATMENT_FLAGS = [
    "feed_map_decouple_m11_treatment",
    "recommended_amenities_2024_treatment_b",
    "filter_redesign_2024_treatment",
    "filter_reordering_2024_roomtype_treatment",
    "p2_category_bar_removal_treatment",
    "selected_filters_2024_treatment",
    "recommended_filters_2024_treatment_b",
    "m13_search_input_phase2_treatment",
    "m13_search_input_services_enabled",
    "m13_2025_experiences_p2_treatment",
]

KNOWN_PLACE_IDS: Dict[str, Tuple[str, str]] = {
    "da nang":     ("ChIJEyolkscZQjERBn5yhkvL8B0", "Da Nang"),
    "đà nẵng":     ("ChIJEyolkscZQjERBn5yhkvL8B0", "Da Nang"),
    "hội an":      ("ChIJd5SFCt4ZQjERVqYaFjqBB0Q", "Hoi An"),
    "hoi an":      ("ChIJd5SFCt4ZQjERVqYaFjqBB0Q", "Hoi An"),
    "ho chi minh": ("ChIJ0T2NLikpdTERKxE8d61aX_E", "Ho Chi Minh City"),
    "hà nội":      ("ChIJs3E7PaNNGTERBILQRBMoZIk", "Hanoi"),
    "hanoi":       ("ChIJs3E7PaNNGTERBILQRBMoZIk", "Hanoi"),
    "nha trang":   ("ChIJLfqwqa-UZzEREkAOH8nBNaE", "Nha Trang"),
    "đà lạt":      ("ChIJ01EMrI8tdTERLB_YfNFQ9xA", "Da Lat"),
    "phu quoc":    ("ChIJYXNmkPbxcDERSFvQ4J9XoQ4", "Phu Quoc"),
    "vung tau":    ("ChIJf1_LZEq9djERTotNJB-eCJY", "Vung Tau"),
}

DISTRICT_TO_CITY: Dict[str, str] = {
    "quận sơn trà":       "Da Nang",
    "quận hải châu":      "Da Nang",
    "quận ngũ hành sơn":  "Da Nang",
    "quận liên chiểu":    "Da Nang",
    "quận thanh khê":     "Da Nang",
    "quận cẩm lệ":        "Da Nang",
    "quận 1":             "Ho Chi Minh",
    "quận 3":             "Ho Chi Minh",
    "quận 7":             "Ho Chi Minh",
    "quận hoàn kiếm":     "Hanoi",
    "quận ba đình":       "Hanoi",
}

# Vietnam public holidays (month, day)
VN_HOLIDAYS = {
    (1, 1), (4, 30), (5, 1), (9, 2),
    (1, 27), (1, 28), (1, 29), (1, 30), (1, 31),  # Tết 2025
    (2, 16), (2, 17), (2, 18), (2, 19), (2, 20),  # Tết 2026
}

# Da Nang seasonality
DA_NANG_PEAK    = {6, 7, 8}
DA_NANG_SHOULDER = {3, 4, 5, 9, 10}
DA_NANG_LOW     = {11, 12, 1, 2}

GRAPHQL_HEADERS = {
    "Content-Type":                     "application/json",
    "User-Agent":                       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/147.0.0.0 Safari/537.36",
    "X-Airbnb-API-Key":                 AIRBNB_API_KEY,
    "X-Airbnb-GraphQL-Platform":        "web",
    "X-Airbnb-GraphQL-Platform-Client": "minimalist-niobe",
    "X-Airbnb-Supports-Airlock-V2":     "true",
    "X-CSRF-Without-Token":             "1",
    "X-Niobe-Short-Circuited":          "true",
    "ect":                              "4g",
    "Referer":                          "https://www.airbnb.com/s/Da-Nang/homes",
}


# ══════════════════════════════════════════════════════════════
# DATA MODELS
# ══════════════════════════════════════════════════════════════

@dataclass
class AnalysisRequest:
    """Stage A output — normalized user input."""
    listing_url:     str
    radius_km:       float = 3.0
    similarity:      SimilarityLevel = SimilarityLevel.MEDIUM
    months_ahead:    int   = 3
    currency:        str   = "USD"
    goal:            StrategyGoal = StrategyGoal.BALANCED
    your_rate:       float = 0.0
    checkin:         str   = ""
    checkout:        str   = ""


@dataclass
class ListingProfile:
    """Stage B output — target listing attributes."""
    url:            str
    listing_id:     str   = ""
    name:           str   = ""
    room_type:      str   = ""
    max_guests:     int   = 0
    bedrooms:       int   = 0
    beds:           int   = 0
    bathrooms:      float = 0.0
    latitude:       float = 0.0
    longitude:      float = 0.0
    city:           str   = ""
    state:          str   = ""
    neighborhood:   str   = ""
    rating:         float = 0.0
    review_count:   int   = 0
    is_superhost:   bool  = False
    badge:          str   = ""
    nightly_rate:   float = 0.0
    currency:       str   = "USD"
    spec_summary:   str   = ""
    extracted_via:  str   = ""


@dataclass
class Competitor:
    """Stage D output — one competitor listing."""
    id:                   str
    name:                 str
    url:                  str
    price_per_night:      float
    currency:             str
    latitude:             float
    longitude:            float
    distance_km:          float
    city:                 str
    bedrooms:             int
    beds:                 int
    rating:               float
    review_count:         int
    is_superhost:         bool
    badge:                str
    estimated_occupancy:  float = 0.0
    # Stage E additions
    similarity_score:     float = 0.0
    tier:                 CompTier = CompTier.CONTEXT
    score_breakdown:      Dict[str, float] = field(default_factory=dict)


@dataclass
class PriceIntelligence:
    """Stage F output — market pricing statistics."""
    comp_count:        int
    direct_count:      int
    secondary_count:   int
    context_count:     int
    # Core stats
    median:            float
    weighted_avg:      float
    trimmed_mean:      float
    p25:               float
    p75:               float
    p10:               float
    p90:               float
    # Derived
    market_band_low:   float
    market_band_high:  float
    suggested_base:    float
    tier_position:     str    # "budget" | "mid" | "premium" | "luxury"
    currency:          str
    # Outlier info
    outliers_removed:  int


@dataclass
class DayStrategy:
    """One day's pricing parameters."""
    date:              str
    demand_level:      str   # peak / high / medium / low
    day_type:          str   # weekday / friday / weekend / holiday
    suggested_price:   float
    min_acceptable:    float
    premium_price:     float
    multiplier:        float
    notes:             str   = ""


@dataclass
class MonthStrategy:
    """Stage G output — one month's strategy."""
    month:             int
    year:              int
    season:            str
    base_rate:         float
    weekday_rate:      float
    weekend_rate:      float
    peak_rate:         float
    floor_rate:        float
    weekend_premium:   float   # percentage
    min_stay_weekday:  int
    min_stay_weekend:  int
    occupancy_target:  float
    revenue_target:    float
    notes:             str     = ""


@dataclass
class StrategyOutput:
    """Stage J — complete analysis output."""
    request:             AnalysisRequest
    listing:             ListingProfile
    comps:               List[Competitor]
    pricing:             PriceIntelligence
    monthly_plans:       List[MonthStrategy]
    sample_days:         List[DayStrategy]
    recommendations:     List[str]
    action_plan:         Dict[str, Any]
    confidence:          Confidence
    confidence_notes:    List[str]
    generated_at:        str


# ══════════════════════════════════════════════════════════════
# GEO UTILITIES
# ══════════════════════════════════════════════════════════════

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def bbox_from_center(lat: float, lon: float, radius_km: float) -> Tuple[float, float, float, float]:
    """Return (ne_lat, ne_lng, sw_lat, sw_lng)."""
    dlat = radius_km / 111.0
    dlon = radius_km / (111.0 * math.cos(math.radians(lat)))
    return (round(lat+dlat, 8), round(lon+dlon, 8),
            round(lat-dlat, 8), round(lon-dlon, 8))


def calc_zoom(radius_km: float, center_lat: float) -> float:
    dlon = radius_km / (111.0 * math.cos(math.radians(center_lat)))
    return round(math.log2(360 / (dlon * 2) * 640 / 256), 6)


def lookup_place(city: str) -> Tuple[str, str]:
    key = city.lower().strip()
    if key in KNOWN_PLACE_IDS:
        return KNOWN_PLACE_IDS[key]
    nfd = unicodedata.normalize("NFD", key)
    ascii_key = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    if ascii_key in KNOWN_PLACE_IDS:
        return KNOWN_PLACE_IDS[ascii_key]
    return ("", city)


def resolve_city(city: str, state: str) -> str:
    """Resolve district names to parent city."""
    for key in [city.lower().strip(), state.lower().strip()]:
        if key in DISTRICT_TO_CITY:
            return DISTRICT_TO_CITY[key]
    pid, display = lookup_place(state or city)
    return display if display else (state or city)


# ══════════════════════════════════════════════════════════════
# STAGE A — INPUT NORMALIZATION
# ══════════════════════════════════════════════════════════════

def stage_a_normalize(
    listing_url: str,
    radius_km: float = 3.0,
    similarity: str = "medium",
    months_ahead: int = 3,
    currency: str = "USD",
    goal: str = "balanced",
    your_rate: float = 0.0,
) -> AnalysisRequest:
    """Validate and normalize user inputs."""
    log.info("Stage A: Normalizing inputs…")

    url = listing_url.strip()
    if not re.search(r'airbnb\.com/rooms/\d+', url):
        raise ValueError(f"Invalid Airbnb listing URL: {url!r}")

    sim_map = {"strict": SimilarityLevel.STRICT,
               "medium": SimilarityLevel.MEDIUM,
               "broad":  SimilarityLevel.BROAD}
    sim = sim_map.get(similarity.lower(), SimilarityLevel.MEDIUM)

    # Auto-adjust radius by similarity if not explicitly set
    if radius_km == 3.0:
        if sim == SimilarityLevel.STRICT:  radius_km = 1.5
        elif sim == SimilarityLevel.BROAD: radius_km = 5.0

    goal_map = {"revenue": StrategyGoal.MAXIMIZE_REVENUE,
                "occupancy": StrategyGoal.MAXIMIZE_OCCUPANCY,
                "balanced": StrategyGoal.BALANCED}

    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    checkin  = tomorrow.strftime("%Y-%m-%d")
    checkout = (tomorrow + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    req = AnalysisRequest(
        listing_url=url,
        radius_km=radius_km,
        similarity=sim,
        months_ahead=max(1, min(12, months_ahead)),
        currency=currency.upper(),
        goal=goal_map.get(goal.lower(), StrategyGoal.BALANCED),
        your_rate=your_rate,
        checkin=checkin,
        checkout=checkout,
    )
    log.info(f"  radius={req.radius_km}km  similarity={sim.value}  goal={req.goal.value}  months={months_ahead}")
    return req


# ══════════════════════════════════════════════════════════════
# STAGE B — LISTING EXTRACTION (Playwright)
# ══════════════════════════════════════════════════════════════

def _dig(obj: Any, *keys, default=None) -> Any:
    cur = obj
    for k in keys:
        try:
            cur = cur[k]
        except (KeyError, IndexError, TypeError):
            return default
    return cur


def _extract_from_next_data(blob: dict) -> Optional[dict]:
    for path in [
        ("props", "pageProps", "bootstrapData", "reduxData", "homePDP", "listingInfo", "listing"),
        ("props", "pageProps", "listing"),
    ]:
        node = _dig(blob, *path)
        if node:
            return node
    return None


def _regex_extract_listing(html: str) -> dict:
    result = {}
    patterns = {
        "latitude":       r'"latitude"\s*:\s*([\d.\-]+)',
        "longitude":      r'"longitude"\s*:\s*([\d.\-]+)',
        "city":           r'"city"\s*:\s*"([^"]+)"',
        "state":          r'"stateCode"\s*:\s*"([^"]+)"',
        "room_type_raw":  r'"roomTypeCategory"\s*:\s*"([^"]+)"',
        "bedrooms":       r'"bedrooms"\s*:\s*(\d+)',
        "beds":           r'"beds"\s*:\s*(\d+)',
        "bathrooms":      r'"bathrooms"\s*:\s*([\d.]+)',
        "personCapacity": r'"personCapacity"\s*:\s*(\d+)',
        "starRating":     r'"starRating"\s*:\s*([\d.]+)',
        "reviewsCount":   r'"reviewsCount"\s*:\s*(\d+)',
        "name":           r'"name"\s*:\s*"([^"]{5,120})"',
    }
    for field_name, pat in patterns.items():
        m = re.search(pat, html)
        if m:
            val = m.group(1)
            try:
                result[field_name] = float(val) if "." in val else int(val) if val.isdigit() else val
            except:
                result[field_name] = val
    return result


def stage_b_extract_listing(req: AnalysisRequest) -> ListingProfile:
    """Extract listing attributes from the Airbnb URL using Playwright."""
    log.info("Stage B: Extracting listing attributes…")

    m = re.search(r'/rooms/(\d+)', req.listing_url)
    listing_id = m.group(1) if m else ""
    profile = ListingProfile(url=req.listing_url, listing_id=listing_id)

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                locale="en-US",
                timezone_id="Asia/Ho_Chi_Minh",
                viewport={"width": 1440, "height": 900},
            )
            page = ctx.new_page()
            page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page.goto(req.listing_url, timeout=60000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except:
                page.wait_for_timeout(5000)

            raw = {}
            # Try __NEXT_DATA__
            nd_el = page.query_selector("script#__NEXT_DATA__")
            if nd_el:
                try:
                    nd = json.loads(nd_el.inner_text())
                    listing_node = _extract_from_next_data(nd)
                    if listing_node:
                        raw = listing_node
                        profile.extracted_via = "next_data"
                except:
                    pass

            if not raw:
                html = page.content()
                raw = _regex_extract_listing(html)
                profile.extracted_via = "regex"

            browser.close()

        _map_raw_to_profile(raw, profile)
    except ImportError:
        log.warning("Playwright not available — listing extraction limited")
        profile.extracted_via = "unavailable"
    except Exception as e:
        log.warning(f"Listing extraction failed: {e}")
        profile.extracted_via = "failed"

    log.info(f"  listing: '{profile.name}' | {profile.room_type} | {profile.bedrooms}BR | {profile.rating}★ | via={profile.extracted_via}")
    return profile


def _map_raw_to_profile(raw: dict, profile: ListingProfile) -> None:
    profile.latitude   = float(_dig(raw, "coordinate", "latitude") or raw.get("latitude", 0))
    profile.longitude  = float(_dig(raw, "coordinate", "longitude") or raw.get("longitude", 0))
    profile.city       = raw.get("city", "")
    profile.state      = raw.get("state", raw.get("stateCode", ""))
    rt = raw.get("roomTypeCategory") or raw.get("room_type_raw", "")
    rt_map = {"entire_home": "Entire home", "private_room": "Private room", "shared_room": "Shared room"}
    profile.room_type  = rt_map.get(rt.lower().replace("/","_").replace(" ","_"), rt)
    profile.max_guests = int(raw.get("personCapacity") or raw.get("guests", 0))
    profile.bedrooms   = int(raw.get("bedrooms", 0))
    profile.beds       = int(raw.get("beds", 0))
    profile.bathrooms  = float(raw.get("bathrooms", 0))
    profile.name       = raw.get("name", "")
    profile.rating     = float(raw.get("starRating") or raw.get("rating", 0))
    profile.review_count = int(raw.get("reviewsCount") or raw.get("review_count", 0))
    profile.is_superhost = bool(raw.get("isSuperHost") or raw.get("is_superhost", False))


# ══════════════════════════════════════════════════════════════
# STAGE C — GEO SCOPE
# ══════════════════════════════════════════════════════════════

@dataclass
class SearchZone:
    ne_lat: float
    ne_lng: float
    sw_lat: float
    sw_lng: float
    radius_km: float
    label: str


def stage_c_geo_scope(listing: ListingProfile, req: AnalysisRequest) -> List[SearchZone]:
    """Create search zones around the listing."""
    log.info("Stage C: Creating geo search zones…")

    if not listing.latitude:
        raise ValueError("Listing has no coordinates — cannot create geo scope")

    lat, lon = listing.latitude, listing.longitude
    r = req.radius_km

    zones = []
    # Primary zone
    ne_lat, ne_lng, sw_lat, sw_lng = bbox_from_center(lat, lon, r)
    zones.append(SearchZone(ne_lat, ne_lng, sw_lat, sw_lng, r, "primary"))

    # For strict mode, also add a tighter inner zone
    if req.similarity == SimilarityLevel.STRICT and r > 1:
        ne2, ne2_lng, sw2, sw2_lng = bbox_from_center(lat, lon, 1.0)
        zones.append(SearchZone(ne2, ne2_lng, sw2, sw2_lng, 1.0, "inner_1km"))

    log.info(f"  {len(zones)} zone(s) | radius={r}km | center=({lat:.4f},{lon:.4f})")
    return zones


# ══════════════════════════════════════════════════════════════
# STAGE D — CANDIDATE RETRIEVAL (GraphQL API)
# ══════════════════════════════════════════════════════════════

def _make_cursor(offset: int) -> Optional[str]:
    if offset == 0:
        return None
    payload = json.dumps({"section_offset": 0, "items_offset": offset, "version": 1}, separators=(",",":"))
    return base64.b64encode(payload.encode()).decode()


def _build_graphql_body(zone: SearchZone, listing: ListingProfile, req: AnalysisRequest, offset: int) -> dict:
    city_name = resolve_city(listing.city, listing.state)
    place_id, display_city = lookup_place(city_name)

    def p(name, *vals):
        return {"filterName": name, "filterValues": list(vals)}

    raw_params = [
        p("adults",              str(max(1, listing.max_guests))),
        p("cdnCacheSafe",        "false"),
        p("channel",             "EXPLORE"),
        p("flexibleTripLengths", "one_week"),
        p("itemsPerGrid",        "18"),
        p("neLat",               str(zone.ne_lat)),
        p("neLng",               str(zone.ne_lng)),
        p("swLat",               str(zone.sw_lat)),
        p("swLng",               str(zone.sw_lng)),
        p("query",               display_city or city_name),
        p("refinementPaths",     "/homes"),
        p("screenSize",          "large"),
        p("searchByMap",         "true"),
        p("searchMode",          "regular_search"),
        p("tabId",               "home_tab"),
        p("version",             "1.8.8"),
        p("zoomLevel",           "14"),
        p("priceFilterInputType","2"),
        p("priceFilterNumNights","1"),
    ]
    if place_id:
        raw_params.append(p("placeId", place_id))
    if listing.bedrooms:
        raw_params.append(p("minBedrooms", str(listing.bedrooms)))
    rt_map = {"Entire home": "Entire home/apt", "Private room": "Private room", "Shared room": "Shared room"}
    rt_encoded = rt_map.get(listing.room_type, "Entire home/apt")
    raw_params.append(p("roomTypes", rt_encoded))
    if req.checkin:
        raw_params += [p("checkin", req.checkin), p("checkout", req.checkout)]

    cursor = _make_cursor(offset)
    search_req: Dict[str, Any] = {
        "metadataOnly": False,
        "requestedPageType": "STAYS_SEARCH",
        "searchType": "user_map_move",
        "treatmentFlags": TREATMENT_FLAGS,
        "rawParams": raw_params,
    }
    if cursor:
        search_req["cursor"] = cursor

    return {
        "operationName": "StaysSearch",
        "variables": {
            "staysSearchRequest":      search_req,
            "staysMapSearchRequestV2": search_req,
            "isLeanTreatment": False,
            "aiSearchEnabled": False,
        },
        "extensions": {
            "persistedQuery": {"version": 1, "sha256Hash": STAYS_SEARCH_HASH}
        },
    }


def _post_graphql(body: dict, retries: int = 3) -> Optional[dict]:
    payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
    ctx = ssl.create_default_context()
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(GRAPHQL_URL, data=payload, headers=GRAPHQL_HEADERS, method="POST")
            with urllib.request.urlopen(req, timeout=30, context=ctx) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            log.warning(f"HTTP {e.code} attempt {attempt}/{retries}")
            if e.code == 403:
                log.error("403 — API key or hash stale. Update AIRBNB_API_KEY / STAYS_SEARCH_HASH")
                return None
            time.sleep(random.uniform(3, 6))
        except Exception as e:
            log.warning(f"Request error attempt {attempt}/{retries}: {e}")
            time.sleep(random.uniform(2, 4))
    return None


def _parse_node(node: dict, origin_lat: float, origin_lon: float, currency: str) -> Optional[Competitor]:
    dsl = node.get("demandStayListing") or {}
    lid_b64 = dsl.get("id", "")
    lid = ""
    if lid_b64:
        try:
            m = re.search(r"(\d+)", base64.b64decode(lid_b64 + "==").decode())
            lid = m.group(1) if m else ""
        except:
            pass

    name = ""
    desc = (dsl.get("description") or {}).get("name") or {}
    name = desc.get("localizedStringWithTranslationPreference", "")
    if not name:
        name = node.get("subtitle", "") or node.get("title", "")
    if not name:
        return None

    coord = (dsl.get("location") or {}).get("coordinate") or {}
    lat = float(coord.get("latitude", 0))
    lon = float(coord.get("longitude", 0))
    dist = round(haversine_km(origin_lat, origin_lon, lat, lon), 2) if lat and lon else -1.0

    sdp  = node.get("structuredDisplayPrice") or {}
    pl   = (sdp.get("primaryLine") or {})
    ps   = pl.get("price", "") or pl.get("accessibilityLabel", "")
    price = 0.0
    if ps:
        pm = re.search(r"[\$₫]?([\d,]+)", ps.replace(",",""))
        if pm:
            price = float(pm.group(1).replace(",",""))

    loc = node.get("avgRatingLocalized", "") or ""
    lm  = re.match(r"([\d.]+)\s*\((\d+)\)", loc)
    rating  = float(lm.group(1)) if lm else 0.0
    reviews = int(lm.group(2))   if lm else 0

    is_superhost, badge = False, ""
    for b in (node.get("badges") or []):
        btype = ((b.get("loggingContext") or {}).get("badgeType") or "").upper()
        text  = (b.get("text") or "").lower()
        if btype == "SUPERHOST" or "superhost" in text:
            is_superhost = True
        elif btype == "GUEST_FAVORITE" or "guest fav" in text:
            badge = "Guest favourite"
        elif btype == "RARE_FIND" or "rare find" in text:
            badge = "Rare find"

    bedrooms, beds = 0, 0
    sc = node.get("structuredContent") or {}
    for item in (sc.get("primaryLine") or []) + (sc.get("mapPrimaryLine") or []):
        body = item.get("body", "") or ""
        if "bedroom" in body:
            bm = re.search(r"(\d+)", body)
            if bm: bedrooms = int(bm.group(1))
        elif "bed" in body:
            bm = re.search(r"(\d+)", body)
            if bm: beds = int(bm.group(1))

    city = ""
    title = node.get("title", "") or ""
    tm = re.search(r" in (.+)$", title)
    if tm: city = tm.group(1)

    occ = min((reviews / 0.72 * 3) / 365, 1.0) if reviews > 0 else 0.0

    return Competitor(
        id=str(lid), name=name, url=f"https://www.airbnb.com/rooms/{lid}" if lid else "",
        price_per_night=price, currency=currency,
        latitude=lat, longitude=lon, distance_km=dist, city=city,
        bedrooms=bedrooms, beds=beds, rating=rating, review_count=reviews,
        is_superhost=is_superhost, badge=badge, estimated_occupancy=round(occ, 2),
    )


def stage_d_retrieve_candidates(
    zones: List[SearchZone],
    listing: ListingProfile,
    req: AnalysisRequest,
    pages_per_zone: int = 3,
) -> List[Competitor]:
    """Retrieve competitors from all search zones."""
    log.info("Stage D: Retrieving candidate listings…")
    all_comps: List[Competitor] = []
    seen_ids: set = set()

    for zone in zones:
        log.info(f"  Zone '{zone.label}'…")
        for page in range(pages_per_zone):
            offset = page * 18
            body = _build_graphql_body(zone, listing, req, offset)
            response = _post_graphql(body)
            if not response:
                log.warning(f"  No response for zone={zone.label} page={page+1}")
                break

            nodes = _dig(response, "data", "presentation", "staysSearch", "results", "searchResults") or []
            page_comps = []
            for node in nodes:
                comp = _parse_node(node, listing.latitude, listing.longitude, req.currency)
                if comp and comp.price_per_night > 0 and comp.id not in seen_ids:
                    seen_ids.add(comp.id)
                    page_comps.append(comp)
            all_comps.extend(page_comps)
            log.info(f"    page {page+1}: {len(page_comps)} new listings (total {len(all_comps)})")

            if len(nodes) < 18:
                break
            if page < pages_per_zone - 1:
                time.sleep(random.uniform(1.0, 2.5))

    log.info(f"  Total candidates: {len(all_comps)}")
    return all_comps


# ══════════════════════════════════════════════════════════════
# STAGE E — SIMILARITY SCORING
# ══════════════════════════════════════════════════════════════

SIMILARITY_WEIGHTS = {
    "distance":       0.25,
    "room_type":      0.20,
    "bedrooms":       0.20,
    "guests":         0.10,
    "rating":         0.10,
    "reviews":        0.07,
    "superhost":      0.05,
    "badge":          0.03,
}

def _score_distance(dist_km: float, radius_km: float) -> float:
    if dist_km < 0: return 0.3
    if dist_km == 0: return 1.0
    return max(0, 1.0 - (dist_km / radius_km))

def _score_bedrooms(comp_br: int, target_br: int) -> float:
    if target_br == 0: return 0.5
    diff = abs(comp_br - target_br)
    if diff == 0: return 1.0
    if diff == 1: return 0.6
    if diff == 2: return 0.3
    return 0.0

def _score_rating(comp_r: float, target_r: float) -> float:
    if target_r == 0: return 0.5
    diff = abs(comp_r - target_r)
    return max(0, 1.0 - diff * 2)

def _score_reviews(comp_rev: int, target_rev: int) -> float:
    if target_rev == 0: return 0.5
    ratio = min(comp_rev, target_rev) / max(comp_rev, target_rev, 1)
    return ratio

def stage_e_similarity(
    comps: List[Competitor],
    listing: ListingProfile,
    req: AnalysisRequest,
) -> List[Competitor]:
    """Score and tier each competitor by similarity to target listing."""
    log.info("Stage E: Computing similarity scores…")
    w = SIMILARITY_WEIGHTS

    for comp in comps:
        breakdown = {}
        breakdown["distance"] = _score_distance(comp.distance_km, req.radius_km)
        breakdown["room_type"] = 1.0 if comp.city == listing.city else 0.5  # proxy
        breakdown["bedrooms"]  = _score_bedrooms(comp.bedrooms, listing.bedrooms)
        breakdown["guests"]    = 1.0 if listing.max_guests == 0 else max(0, 1 - abs(0) / 4)
        breakdown["rating"]    = _score_rating(comp.rating, listing.rating)
        breakdown["reviews"]   = _score_reviews(comp.review_count, listing.review_count)
        breakdown["superhost"] = 1.0 if comp.is_superhost == listing.is_superhost else 0.5
        breakdown["badge"]     = 1.0 if comp.badge == listing.badge else 0.7

        score = sum(breakdown[k] * w[k] for k in w)
        comp.similarity_score = round(score, 4)
        comp.score_breakdown  = {k: round(v, 3) for k, v in breakdown.items()}

        if score >= 0.72:
            comp.tier = CompTier.DIRECT
        elif score >= 0.50:
            comp.tier = CompTier.SECONDARY
        else:
            comp.tier = CompTier.CONTEXT

    comps.sort(key=lambda c: c.similarity_score, reverse=True)
    direct = sum(1 for c in comps if c.tier == CompTier.DIRECT)
    secondary = sum(1 for c in comps if c.tier == CompTier.SECONDARY)
    context = sum(1 for c in comps if c.tier == CompTier.CONTEXT)
    log.info(f"  direct={direct}  secondary={secondary}  context={context}")
    return comps


# ══════════════════════════════════════════════════════════════
# STAGE F — PRICE INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def _percentile(data: List[float], p: float) -> float:
    if not data: return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(sorted_data) - 1)
    return sorted_data[lo] + (sorted_data[hi] - sorted_data[lo]) * (k - lo)

def _trimmed_mean(data: List[float], trim_pct: float = 10.0) -> float:
    if not data: return 0.0
    s = sorted(data)
    cut = max(1, int(len(s) * trim_pct / 100))
    trimmed = s[cut:-cut] if len(s) > cut * 2 else s
    return sum(trimmed) / len(trimmed)

def _weighted_avg(comps: List[Competitor]) -> float:
    """Weight by similarity score."""
    total_w = sum(c.similarity_score for c in comps if c.price_per_night > 0)
    if not total_w: return 0.0
    return sum(c.price_per_night * c.similarity_score for c in comps if c.price_per_night > 0) / total_w

def stage_f_price_intelligence(
    comps: List[Competitor],
    listing: ListingProfile,
    req: AnalysisRequest,
) -> PriceIntelligence:
    """Compute robust market pricing statistics."""
    log.info("Stage F: Computing price intelligence…")

    prices = [c.price_per_night for c in comps if c.price_per_night > 0]
    if not prices:
        raise ValueError("No valid prices found in competitor set")

    # Remove outliers: below p5 or above p95
    p5  = _percentile(prices, 5)
    p95 = _percentile(prices, 95)
    clean = [p for p in prices if p5 <= p <= p95]
    outliers_removed = len(prices) - len(clean)

    # Work with cleaned prices
    clean_comps = [c for c in comps if p5 <= c.price_per_night <= p95]
    direct_comps = [c for c in clean_comps if c.tier == CompTier.DIRECT]
    prices_direct = [c.price_per_night for c in direct_comps] if direct_comps else clean

    median      = _percentile(clean, 50)
    w_avg       = _weighted_avg(clean_comps)
    trimmed     = _trimmed_mean(clean)
    p25         = _percentile(clean, 25)
    p75         = _percentile(clean, 75)
    p10         = _percentile(clean, 10)
    p90         = _percentile(clean, 90)

    # Suggested base: weighted blend favouring direct comps
    if direct_comps:
        direct_median = _percentile(prices_direct, 50)
        suggested = direct_median * 0.6 + median * 0.4
    else:
        suggested = trimmed * 0.7 + median * 0.3

    # Tier position of listing
    your_rate = req.your_rate or suggested
    if your_rate <= p25:     tier_pos = "budget"
    elif your_rate <= median:   tier_pos = "mid"
    elif your_rate <= p75:   tier_pos = "premium"
    else:                    tier_pos = "luxury"

    direct_count    = sum(1 for c in comps if c.tier == CompTier.DIRECT)
    secondary_count = sum(1 for c in comps if c.tier == CompTier.SECONDARY)
    context_count   = sum(1 for c in comps if c.tier == CompTier.CONTEXT)

    result = PriceIntelligence(
        comp_count=len(comps),
        direct_count=direct_count,
        secondary_count=secondary_count,
        context_count=context_count,
        median=round(median, 2),
        weighted_avg=round(w_avg, 2),
        trimmed_mean=round(trimmed, 2),
        p25=round(p25, 2), p75=round(p75, 2),
        p10=round(p10, 2), p90=round(p90, 2),
        market_band_low=round(p25, 2),
        market_band_high=round(p75, 2),
        suggested_base=round(suggested, 2),
        tier_position=tier_pos,
        currency=req.currency,
        outliers_removed=outliers_removed,
    )
    log.info(f"  median={median:.0f}  suggested={suggested:.0f}  band=[{p25:.0f},{p75:.0f}]  tier={tier_pos}")
    return result


# ══════════════════════════════════════════════════════════════
# STAGE G — MONTHLY STRATEGY
# ══════════════════════════════════════════════════════════════

DEMAND_MULTIPLIERS = {
    "peak":    {"weekday": 1.45, "weekend": 1.65, "holiday": 1.80},
    "high":    {"weekday": 1.15, "weekend": 1.35, "holiday": 1.50},
    "medium":  {"weekday": 0.98, "weekend": 1.18, "holiday": 1.30},
    "low":     {"weekday": 0.78, "weekend": 0.90, "holiday": 1.10},
}

def _get_season(month: int, city: str) -> str:
    city_lower = city.lower()
    if any(x in city_lower for x in ["da nang", "danang", "hoi an", "đà nẵng", "hội an"]):
        if month in DA_NANG_PEAK:     return "peak"
        if month in DA_NANG_SHOULDER: return "high"
        return "low"
    # Generic Vietnam
    if month in {6, 7, 8, 12, 1}:    return "high"
    if month in {4, 5, 9, 10}:        return "medium"
    return "low"

def stage_g_monthly_strategy(
    pricing: PriceIntelligence,
    listing: ListingProfile,
    req: AnalysisRequest,
) -> List[MonthStrategy]:
    """Generate month-by-month pricing strategy."""
    log.info("Stage G: Generating monthly strategy…")
    today = datetime.date.today()
    base = pricing.suggested_base
    plans = []

    for i in range(req.months_ahead):
        target_date = today + datetime.timedelta(days=30 * i)
        month, year = target_date.month, target_date.year
        season = _get_season(month, listing.city or listing.state)
        mults = DEMAND_MULTIPLIERS[season]

        weekday_rate = round(base * mults["weekday"], 2)
        weekend_rate = round(base * mults["weekend"], 2)
        peak_rate    = round(base * mults["holiday"], 2)
        floor_rate   = round(base * 0.70, 2)
        w_premium    = round((weekend_rate / weekday_rate - 1) * 100, 1) if weekday_rate > 0 else 0

        # Min stay: tighten in peak to protect high-value nights
        min_stay_wd  = 1 if season == "low" else 2
        min_stay_wkd = 2 if season == "low" else 3 if season == "peak" else 2

        # Occupancy target by season
        occ_target = {"peak": 0.85, "high": 0.72, "medium": 0.65, "low": 0.50}[season]

        # Revenue target = avg_rate × nights_in_month × occ_target
        import calendar
        days_in_month = calendar.monthrange(year, month)[1]
        avg_rate = (weekday_rate * 5 + weekend_rate * 2) / 7
        rev_target = round(avg_rate * days_in_month * occ_target, 0)

        month_names = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        notes = f"{month_names[month]} {year} — {season.upper()} season"
        if season == "peak":
            notes += " | Raise minimum stay to 3 nights on weekends | Max revenue focus"
        elif season == "low":
            notes += " | Lower floor to fill gaps | Consider 1-night minimum"

        plans.append(MonthStrategy(
            month=month, year=year, season=season,
            base_rate=round(base, 2),
            weekday_rate=weekday_rate,
            weekend_rate=weekend_rate,
            peak_rate=peak_rate,
            floor_rate=floor_rate,
            weekend_premium=w_premium,
            min_stay_weekday=min_stay_wd,
            min_stay_weekend=min_stay_wkd,
            occupancy_target=occ_target,
            revenue_target=rev_target,
            notes=notes,
        ))

    log.info(f"  {len(plans)} month plans generated")
    return plans


# ══════════════════════════════════════════════════════════════
# STAGE H — SAMPLE DAY INTELLIGENCE
# ══════════════════════════════════════════════════════════════

def _demand_for_day(day: datetime.date, city: str) -> Tuple[str, str]:
    """Return (demand_level, day_type)."""
    if (day.month, day.day) in VN_HOLIDAYS:
        return "peak", "holiday"
    season = _get_season(day.month, city)
    if day.weekday() == 4:   day_type = "friday"
    elif day.weekday() >= 5: day_type = "weekend"
    else:                    day_type = "weekday"

    if season == "peak" and day_type in ("friday","weekend"): demand = "peak"
    elif season == "peak":                                     demand = "high"
    elif day_type in ("friday","weekend"):                     demand = "high"
    elif season == "high":                                     demand = "medium"
    else:                                                      demand = "low"
    return demand, day_type

def stage_h_sample_days(
    pricing: PriceIntelligence,
    monthly: List[MonthStrategy],
    listing: ListingProfile,
) -> List[DayStrategy]:
    """Stratified sampling of representative days."""
    log.info("Stage H: Sampling representative days…")
    today = datetime.date.today()
    samples: List[DayStrategy] = []
    city = listing.city or listing.state

    for plan in monthly:
        import calendar
        days_in_month = calendar.monthrange(plan.year, plan.month)[1]
        all_days = [datetime.date(plan.year, plan.month, d) for d in range(1, days_in_month+1)
                    if datetime.date(plan.year, plan.month, d) > today]
        if not all_days:
            continue

        weekdays = [d for d in all_days if d.weekday() < 4]
        fridays  = [d for d in all_days if d.weekday() == 4]
        weekends = [d for d in all_days if d.weekday() >= 5]
        holidays = [d for d in all_days if (d.month, d.day) in VN_HOLIDAYS]

        picks = []
        if weekdays: picks.append(random.choice(weekdays))
        if fridays:  picks.append(random.choice(fridays))
        if weekends: picks.append(random.choice(weekends))
        if holidays: picks.append(random.choice(holidays))

        for day in picks:
            demand, day_type = _demand_for_day(day, city)
            mults = DEMAND_MULTIPLIERS[demand]
            mult_key = "holiday" if day_type == "holiday" else "weekend" if day_type in ("friday","weekend") else "weekday"
            mult = mults[mult_key]
            suggested = round(pricing.suggested_base * mult, 2)
            samples.append(DayStrategy(
                date=str(day),
                demand_level=demand,
                day_type=day_type,
                suggested_price=suggested,
                min_acceptable=round(pricing.suggested_base * 0.70, 2),
                premium_price=round(suggested * 1.15, 2),
                multiplier=mult,
                notes=f"{demand.upper()} demand — {day.strftime('%A')}",
            ))

    log.info(f"  {len(samples)} sample days generated")
    return samples


# ══════════════════════════════════════════════════════════════
# STAGE I — STRATEGY SYNTHESIS
# ══════════════════════════════════════════════════════════════

def stage_i_synthesis(
    listing: ListingProfile,
    comps: List[Competitor],
    pricing: PriceIntelligence,
    monthly: List[MonthStrategy],
    req: AnalysisRequest,
) -> Tuple[List[str], Dict[str, Any], Confidence, List[str]]:
    """Generate actionable recommendations and assess confidence."""
    log.info("Stage I: Synthesizing strategy…")
    recs: List[str] = []
    confidence_notes: List[str] = []

    # ── Positioning ──
    your = req.your_rate or pricing.suggested_base
    vs_market = your - pricing.median
    pct_diff  = (vs_market / pricing.median * 100) if pricing.median else 0

    if pct_diff < -20:
        recs.append(f"Your rate is {abs(pct_diff):.0f}% below median — strong room to raise 15-20% without losing bookings")
    elif pct_diff > 25:
        recs.append(f"Your rate is {pct_diff:.0f}% above median — monitor occupancy; reduce if bookings slow")
    else:
        recs.append(f"Your rate is within normal market range (+{pct_diff:.0f}% vs median) — focus on conversion (photos, title)")

    # ── Weekend premium ──
    avg_wprem = sum(m.weekend_premium for m in monthly) / len(monthly) if monthly else 0
    recs.append(f"Apply a {avg_wprem:.0f}% weekend premium (market supports it) — Fri-Sun minimum +{avg_wprem:.0f}%")

    # ── Low season floor ──
    low_plan = next((m for m in monthly if m.season == "low"), None)
    if low_plan:
        recs.append(f"Low season floor: {pricing.currency} {low_plan.floor_rate:.0f}/night — don't drop below this to protect brand value")

    # ── Minimum stay ──
    peak_plan = next((m for m in monthly if m.season == "peak"), None)
    if peak_plan:
        recs.append(f"Peak season: enforce {peak_plan.min_stay_weekend}-night minimum on weekends to avoid high-value gaps")

    # ── Superhost / badge ──
    superhost_pct = sum(1 for c in comps if c.is_superhost) / len(comps) if comps else 0
    if superhost_pct > 0.4 and not listing.is_superhost:
        recs.append(f"{superhost_pct:.0%} of comps are Superhosts — earning it could justify 8-12% price premium")

    # ── Review gap ──
    avg_rev = sum(c.review_count for c in comps) / len(comps) if comps else 0
    if listing.review_count < avg_rev * 0.4:
        recs.append(f"Your reviews ({listing.review_count}) are well below market avg ({avg_rev:.0f}) — prioritize guest experience to build social proof")

    # ── Goal-specific ──
    if req.goal == StrategyGoal.MAXIMIZE_REVENUE:
        recs.append("Revenue focus: target peak/high nights at full rate; use last-minute discounts only for low-demand gaps")
    elif req.goal == StrategyGoal.MAXIMIZE_OCCUPANCY:
        recs.append("Occupancy focus: price at p25–p40 band; activate instant-book; reduce minimum stay in low season to 1 night")
    else:
        recs.append("Balanced: hold at market median in peak, discount 5-10% in low season; review monthly")

    # ── Price sensitivity estimate ──
    recs.append(f"Price sensitivity: +5% ≈ -2-4% bookings; -5% ≈ +3-6% bookings (market elasticity estimate for this segment)")

    # ── Confidence ──
    direct_count = pricing.direct_count
    if direct_count >= 10:
        conf = Confidence.HIGH
        confidence_notes.append(f"High confidence: {direct_count} direct comps found with strong similarity")
    elif direct_count >= 4:
        conf = Confidence.MEDIUM
        confidence_notes.append(f"Medium confidence: only {direct_count} direct comps; pricing band is wider")
    else:
        conf = Confidence.LOW
        confidence_notes.append(f"Low confidence: <4 direct comps — treat recommendations as directional only")

    if pricing.outliers_removed > 0:
        confidence_notes.append(f"{pricing.outliers_removed} outlier listings removed from price calculation")
    if not listing.latitude:
        confidence_notes.append("WARNING: listing coordinates missing — geo radius may be inaccurate")
    if listing.review_count == 0:
        confidence_notes.append("New listing detected — occupancy estimates are speculative")

    action_plan = {
        "immediate": [
            f"Set base rate: {req.currency} {pricing.suggested_base:.0f}/night",
            f"Activate weekend premium: +{avg_wprem:.0f}%",
            f"Set low-season floor: {req.currency} {pricing.p25:.0f}/night",
        ],
        "this_week": [
            "Update photos if below 15 high-quality images",
            "Enable smart pricing within ±10% of suggested rate",
            "Review and respond to any unanswered messages",
        ],
        "this_month": [
            f"Block high-demand dates for {'min 3 nights' if peak_plan else 'min 2 nights'}",
            "Add monthly discount (10-15%) for stays 28+ nights in low season",
            "Monitor competitor pricing weekly and adjust if needed",
        ],
    }

    log.info(f"  Confidence: {conf.value} | {len(recs)} recommendations")
    return recs, action_plan, conf, confidence_notes


# ══════════════════════════════════════════════════════════════
# STAGE J — OUTPUT
# ══════════════════════════════════════════════════════════════

def stage_j_output(
    req: AnalysisRequest,
    listing: ListingProfile,
    comps: List[Competitor],
    pricing: PriceIntelligence,
    monthly: List[MonthStrategy],
    sample_days: List[DayStrategy],
    recs: List[str],
    action_plan: Dict[str, Any],
    confidence: Confidence,
    confidence_notes: List[str],
) -> StrategyOutput:
    return StrategyOutput(
        request=req,
        listing=listing,
        comps=comps,
        pricing=pricing,
        monthly_plans=monthly,
        sample_days=sample_days,
        recommendations=recs,
        action_plan=action_plan,
        confidence=confidence,
        confidence_notes=confidence_notes,
        generated_at=datetime.datetime.now().isoformat(),
    )


def save_output(output: StrategyOutput, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    def _serialise(obj):
        if hasattr(obj, '__dict__'):
            return {k: _serialise(v) for k, v in obj.__dict__.items()}
        if isinstance(obj, (list, tuple)):
            return [_serialise(i) for i in obj]
        if isinstance(obj, dict):
            return {k: _serialise(v) for k, v in obj.items()}
        if isinstance(obj, Enum):
            return obj.value
        return obj

    data = _serialise(output)

    # JSON
    (out_dir / "strategy.json").write_text(
        json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    log.info(f"Saved → {out_dir / 'strategy.json'}")

    # CSV — comps
    import csv, io
    buf = io.StringIO()
    if output.comps:
        writer = csv.DictWriter(buf, fieldnames=[
            "id","name","price_per_night","currency","distance_km","bedrooms",
            "rating","review_count","is_superhost","badge","similarity_score","tier","url"
        ])
        writer.writeheader()
        for c in output.comps:
            writer.writerow({
                "id": c.id, "name": c.name, "price_per_night": c.price_per_night,
                "currency": c.currency, "distance_km": c.distance_km,
                "bedrooms": c.bedrooms, "rating": c.rating, "review_count": c.review_count,
                "is_superhost": c.is_superhost, "badge": c.badge,
                "similarity_score": c.similarity_score, "tier": c.tier.value, "url": c.url
            })
    (out_dir / "comps.csv").write_text(buf.getvalue(), encoding="utf-8")

    # Human-readable report
    cur = output.pricing.currency
    p = output.pricing
    lines = [
        "=" * 60,
        "  AIRBNB REVENUE STRATEGY REPORT",
        f"  Generated: {output.generated_at[:19]}",
        "=" * 60,
        "",
        "LISTING",
        f"  Name     : {output.listing.name}",
        f"  Type     : {output.listing.room_type}",
        f"  Specs    : {output.listing.bedrooms}BR · {output.listing.beds} beds · {output.listing.bathrooms} baths",
        f"  Location : {output.listing.city}, {output.listing.state}",
        f"  Rating   : {output.listing.rating}★ ({output.listing.review_count} reviews)",
        f"  Coords   : {output.listing.latitude:.5f}, {output.listing.longitude:.5f}",
        "",
        "MARKET PRICING",
        f"  Comps    : {p.comp_count} total ({p.direct_count} direct / {p.secondary_count} secondary / {p.context_count} context)",
        f"  Median   : {cur} {p.median:.0f}",
        f"  Weighted avg: {cur} {p.weighted_avg:.0f}",
        f"  Band P25–P75: {cur} {p.p25:.0f} – {cur} {p.p75:.0f}",
        f"  Suggested base: {cur} {p.suggested_base:.0f}",
        f"  Your tier: {p.tier_position.upper()}",
        "",
        "MONTHLY PLAN",
    ]
    for m in output.monthly_plans:
        lines.append(f"  {m.notes}")
        lines.append(f"    Weekday {cur}{m.weekday_rate:.0f}  Weekend {cur}{m.weekend_rate:.0f}  Peak {cur}{m.peak_rate:.0f}  Floor {cur}{m.floor_rate:.0f}")
        lines.append(f"    Occ target {m.occupancy_target:.0%}  Rev target {cur}{m.revenue_target:.0f}")

    lines += ["", "RECOMMENDATIONS"]
    for i, r in enumerate(output.recommendations, 1):
        lines.append(f"  {i}. {r}")

    lines += ["", "ACTION PLAN"]
    for section, items in output.action_plan.items():
        lines.append(f"  {section.upper()}:")
        for item in items:
            lines.append(f"    • {item}")

    lines += ["", f"CONFIDENCE: {output.confidence.value.upper()}"]
    for note in output.confidence_notes:
        lines.append(f"  • {note}")
    lines.append("")

    (out_dir / "strategy_report.txt").write_text("\n".join(lines), encoding="utf-8")
    log.info(f"Saved → {out_dir / 'strategy_report.txt'}")


# ══════════════════════════════════════════════════════════════
# FULL PIPELINE RUNNER
# ══════════════════════════════════════════════════════════════

def run_pipeline(
    listing_url: str,
    radius_km: float = 3.0,
    similarity: str = "medium",
    months_ahead: int = 3,
    currency: str = "USD",
    goal: str = "balanced",
    your_rate: float = 0.0,
    out_dir: Optional[Path] = None,
) -> StrategyOutput:
    log.info("=" * 55)
    log.info("  Airbnb Revenue Strategy Pipeline")
    log.info("=" * 55)

    # A
    req = stage_a_normalize(listing_url, radius_km, similarity, months_ahead, currency, goal, your_rate)

    # B
    listing = stage_b_extract_listing(req)
    if not listing.latitude:
        log.warning("No coordinates from listing — using Da Nang defaults")
        listing.latitude, listing.longitude = 16.06147, 108.24408
        listing.city = "Da Nang"

    # If listing has no rate, use provided
    if your_rate and not listing.nightly_rate:
        listing.nightly_rate = your_rate
        req.your_rate = your_rate

    # C
    zones = stage_c_geo_scope(listing, req)

    # D
    comps = stage_d_retrieve_candidates(zones, listing, req)
    if not comps:
        raise RuntimeError("No competitors found — check API key/hash or try a different radius")

    # E
    comps = stage_e_similarity(comps, listing, req)

    # F
    pricing = stage_f_price_intelligence(comps, listing, req)

    # G
    monthly = stage_g_monthly_strategy(pricing, listing, req)

    # H
    sample_days = stage_h_sample_days(pricing, monthly, listing)

    # I
    recs, action_plan, confidence, conf_notes = stage_i_synthesis(listing, comps, pricing, monthly, req)

    # J
    output = stage_j_output(req, listing, comps, pricing, monthly, sample_days, recs, action_plan, confidence, conf_notes)

    if out_dir:
        save_output(output, out_dir)

    return output


# ══════════════════════════════════════════════════════════════
# INTERACTIVE MAIN
# ══════════════════════════════════════════════════════════════

def main():
    print("=" * 55)
    print("  Airbnb Revenue Strategy Pipeline")
    print("=" * 55)

    url = input("\nAirbnb listing URL: ").strip()
    if not url:
        url = "https://www.airbnb.com/rooms/730105454611509413"
        print(f"  Using demo: {url}")

    radius_s = input("Radius km [3]: ").strip()
    radius_km = float(radius_s) if radius_s else 3.0

    sim_s = input("Similarity [strict/medium/broad, default medium]: ").strip() or "medium"
    months_s = input("Months ahead [3]: ").strip()
    months = int(months_s) if months_s else 3

    cur_s = input("Currency [USD]: ").strip().upper() or "USD"
    goal_s = input("Goal [revenue/occupancy/balanced]: ").strip() or "balanced"
    rate_s = input(f"Your current rate ({cur_s}, 0=unknown): ").strip()
    your_rate = float(rate_s.replace(",","")) if rate_s else 0.0

    out_dir = current_dir / "strategy"
    print(f"\nRunning pipeline… output → {out_dir}\n")

    output = run_pipeline(url, radius_km, sim_s, months, cur_s, goal_s, your_rate, out_dir)

    # Quick summary
    p = output.pricing
    print(f"\n{'='*55}")
    print(f"  RESULT SUMMARY")
    print(f"{'='*55}")
    print(f"  Listing     : {output.listing.name[:45]}")
    print(f"  Comps found : {p.comp_count} ({p.direct_count} direct)")
    print(f"  Market median: {p.currency} {p.median:.0f}")
    print(f"  Suggested   : {p.currency} {p.suggested_base:.0f}")
    print(f"  Band        : {p.currency} {p.p25:.0f} – {p.currency} {p.p75:.0f}")
    print(f"  Confidence  : {output.confidence.value.upper()}")
    print(f"\n  Top recommendations:")
    for i, r in enumerate(output.recommendations[:4], 1):
        print(f"  {i}. {r}")
    print(f"\n  Full report → {out_dir / 'strategy_report.txt'}")


# ════════════════════════════════════════════════════════════
# PATCHES — airbnb_pipeline_patches.py merged below

import calendar  # noqa — needed by get_stratified_sample_dates, stage_g_monthly_strategy_patched
# ════════════════════════════════════════════════════════════

def extract_specs_from_html(html: str) -> Dict[str, Any]:
    """
    PATCH: Multi-source spec extraction.
    
    Problem: regex '"bedrooms":N' only works when Airbnb embeds it in JSON.
    When the page uses server-rendered HTML or a different schema,
    the field is missing. Solution: try 6 different extraction strategies.
    
    Priority order (most → least reliable):
      1. Overview pill text: "2 bedrooms · 3 beds · 1 bath"
      2. JSON label fields: "bedroomLabel":"2 bedrooms"
      3. JSON numeric fields: "bedrooms":2
      4. SEO/schema.org fields: "numberOfRooms":2
      5. Description text patterns
      6. Title text patterns
    """
    result = {}

    # ── Strategy 1: Overview pill line (most reliable — always rendered) ──
    # Airbnb renders this as "X bedrooms · Y beds · Z baths" in DOM/og:description
    overview_patterns = [
        r'(\d+)\s+bedroom[s]?\s*[·•·]\s*(\d+)\s+bed[s]?\s*[·•·]\s*([\d.]+)\s+bath',
        r'(\d+)\s+bedroom[s]?.*?(\d+)\s+bed[s]?.*?([\d.]+)\s+bath',
        r'Bedroom[s]?:\s*(\d+).*?Bed[s]?:\s*(\d+).*?Bath',
    ]
    for pat in overview_patterns:
        m = re.search(pat, html, re.IGNORECASE | re.DOTALL)
        if m and len(m.groups()) >= 2:
            if not result.get("bedrooms"):
                result["bedrooms"] = int(m.group(1))
            if not result.get("beds") and len(m.groups()) >= 2:
                result["beds"] = int(m.group(2))
            if not result.get("bathrooms") and len(m.groups()) >= 3:
                result["bathrooms"] = float(m.group(3))
            break

    # ── Strategy 2: JSON label strings ──
    label_patterns = {
        "bedrooms":  [r'"bedroomLabel"\s*:\s*"(\d+)', r'"bedroomsLabel"\s*:\s*"(\d+)'],
        "beds":      [r'"bedLabel"\s*:\s*"(\d+)',      r'"bedsLabel"\s*:\s*"(\d+)'],
        "bathrooms": [r'"bathroomLabel"\s*:\s*"([\d.]+)',r'"bathroomsLabel"\s*:\s*"([\d.]+)'],
    }
    for field_name, patterns in label_patterns.items():
        if result.get(field_name):
            continue
        for pat in patterns:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                val = m.group(1)
                result[field_name] = float(val) if "." in val else int(val)
                break

    # ── Strategy 3: Direct JSON numeric ──
    numeric_patterns = {
        "bedrooms":       r'"bedrooms"\s*:\s*(\d+)',
        "beds":           r'"beds"\s*:\s*(\d+)',
        "bathrooms":      r'"bathrooms"\s*:\s*([\d.]+)',
        "personCapacity": r'"personCapacity"\s*:\s*(\d+)',
        "maxGuests":      r'"maxGuests"\s*:\s*(\d+)',
    }
    for field_name, pat in numeric_patterns.items():
        if result.get(field_name):
            continue
        m = re.search(pat, html)
        if m:
            val = m.group(1)
            result[field_name] = float(val) if "." in val else int(val)

    # ── Strategy 4: Schema.org / SEO fields ──
    seo_patterns = {
        "bedrooms": [r'"numberOfRooms"\s*:\s*(\d+)', r'"numberOfBedrooms"\s*:\s*(\d+)'],
        "bathrooms": [r'"numberOfBathroomsTotal"\s*:\s*([\d.]+)', r'"numberOfBathrooms"\s*:\s*([\d.]+)'],
    }
    for field_name, patterns in seo_patterns.items():
        if result.get(field_name):
            continue
        for pat in patterns:
            m = re.search(pat, html)
            if m:
                val = m.group(1)
                result[field_name] = float(val) if "." in val else int(val)
                break

    # ── Strategy 5: Individual text patterns (fallback) ──
    if not result.get("bedrooms"):
        m = re.search(r'(\d+)\s+bedroom', html, re.IGNORECASE)
        if m: result["bedrooms"] = int(m.group(1))

    if not result.get("beds"):
        m = re.search(r'(\d+)\s+bed(?!room)', html, re.IGNORECASE)
        if m: result["beds"] = int(m.group(1))

    if not result.get("bathrooms"):
        m = re.search(r'([\d.]+)\s+bath', html, re.IGNORECASE)
        if m: result["bathrooms"] = float(m.group(1))

    # ── Strategy 6: guests from guestLabel ──
    if not result.get("personCapacity"):
        m = re.search(r'"guestLabel"\s*:\s*"(\d+)', html)
        if not m:
            m = re.search(r'(\d+)\s+guest', html, re.IGNORECASE)
        if m:
            result["personCapacity"] = int(m.group(1))

    return result



def _fetch_listing_rating(listing_id: str, headless: bool = True) -> Tuple[float, int]:
    """
    ADDED: Fetch rating and review count from the /reviews sub-page.
    Airbnb embeds schema.org JSON with "ratingValue" and "ratingCount"
    which is more reliable than parsing the main listing page.
    Returns (rating, review_count) or (0.0, 0) on failure.
    """
    url = f"https://www.airbnb.com/rooms/{listing_id}/reviews"
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                locale="en-US", timezone_id="Asia/Ho_Chi_Minh",
            )
            page = ctx.new_page()
            page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page.goto(url, timeout=45000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except:
                page.wait_for_timeout(3000)
            html = page.content()
            browser.close()

        rating, count = 0.0, 0

        # Pattern 1: schema.org JSON-LD  "ratingValue":4.96,"ratingCount":"170"
        m = re.search(r'"ratingValue"\s*:\s*([\d.]+)', html)
        if m:
            rating = float(m.group(1))

        m = re.search(r'"ratingCount"\s*:\s*"?(\d+)"?', html)
        if m:
            count = int(m.group(1))

        # Pattern 2: reviewsCount in JSON blob
        if count == 0:
            m = re.search(r'"reviewsCount"\s*:\s*(\d+)', html)
            if m:
                count = int(m.group(1))

        # Pattern 3: starRating
        if rating == 0.0:
            m = re.search(r'"starRating"\s*:\s*([\d.]+)', html)
            if m:
                rating = float(m.group(1))

        # Pattern 4: visible text "4.96 (170 reviews)"
        if rating == 0.0 or count == 0:
            m = re.search(r'([\d.]+)\s*\((\d+)\s*review', html, re.IGNORECASE)
            if m:
                if rating == 0.0:
                    rating = float(m.group(1))
                if count == 0:
                    count = int(m.group(2))

        log.info(f"  Reviews page: rating={rating} count={count}")
        return rating, count

    except Exception as e:
        log.warning(f"  Reviews page fetch failed: {e}")
        return 0.0, 0

def stage_b_extract_listing_patched(req, listing_profile_class, dig_fn):
    """
    PATCH: Drop-in replacement for stage_b_extract_listing.
    Adds multi-source spec extraction and DOM scraping for overview text.
    """
    import re as _re

    m = _re.search(r'/rooms/(\d+)', req.listing_url)
    listing_id = m.group(1) if m else ""
    profile = listing_profile_class(url=req.listing_url, listing_id=listing_id)

    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                locale="en-US",
                timezone_id="Asia/Ho_Chi_Minh",
                viewport={"width": 1440, "height": 900},
            )
            page = ctx.new_page()
            page.add_init_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            page.goto(req.listing_url, timeout=60000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except:
                page.wait_for_timeout(5000)

            html = page.content()
            raw = {}

            # Try __NEXT_DATA__ first
            nd_el = page.query_selector("script#__NEXT_DATA__")
            if nd_el:
                try:
                    nd = json.loads(nd_el.inner_text())
                    # Walk to listing node
                    for path in [
                        ("props","pageProps","bootstrapData","reduxData","homePDP","listingInfo","listing"),
                        ("props","pageProps","listing"),
                    ]:
                        node = dig_fn(nd, *path)
                        if node:
                            raw = node
                            profile.extracted_via = "next_data"
                            break
                except:
                    pass

            # Always layer in multi-source spec extraction
            spec_data = extract_specs_from_html(html)

            # DOM: try the overview section directly
            # Airbnb renders: "2 bedrooms · 3 beds · 1 bath" in ol._1qsawv5 or similar
            overview_selectors = [
                "ol._1qsawv5 li",
                "div[data-testid='listing-overview'] li",
                "section[data-testid='listing-overview'] span",
                "div._tqmy57 span",
                "ul._1qsawv5 li",
            ]
            for sel in overview_selectors:
                els = page.query_selector_all(sel)
                if els:
                    texts = [e.inner_text().strip() for e in els if e.inner_text().strip()]
                    overview_text = " · ".join(texts)
                    ov_specs = extract_specs_from_html(overview_text)
                    spec_data.update({k:v for k,v in ov_specs.items() if k not in spec_data})
                    break

            # Merge: JSON raw wins, spec_data fills gaps
            for k, v in spec_data.items():
                if k not in raw or not raw.get(k):
                    raw[k] = v

            # Rating from page if not in JSON
            if not raw.get("starRating"):
                rating_el = page.query_selector("span[aria-label*='rating'], span[aria-label*='star']")
                if rating_el:
                    aria = rating_el.get_attribute("aria-label") or ""
                    rm = _re.search(r"([\d.]+)", aria)
                    if rm: raw["starRating"] = float(rm.group(1))

            browser.close()

        # ADDED: If rating/reviews still missing, fetch from /reviews sub-page
        if (not raw.get("starRating") or not raw.get("reviewsCount")) and listing_id:
            log.info("  Fetching rating from /reviews page…")
            r_rating, r_count = _fetch_listing_rating(listing_id, headless=True)
            if r_rating and not raw.get("starRating"):
                raw["starRating"] = r_rating
            if r_count and not raw.get("reviewsCount"):
                raw["reviewsCount"] = r_count

        # Map to profile
        profile.latitude   = float(dig_fn(raw,"coordinate","latitude") or raw.get("latitude", 0))
        profile.longitude  = float(dig_fn(raw,"coordinate","longitude") or raw.get("longitude", 0))
        profile.city       = raw.get("city", "")
        profile.state      = raw.get("state", raw.get("stateCode",""))
        rt = raw.get("roomTypeCategory") or raw.get("room_type_raw","")
        rt_map = {"entire_home":"Entire home","private_room":"Private room","shared_room":"Shared room"}
        profile.room_type  = rt_map.get(rt.lower().replace("/","_").replace(" ","_"), rt)
        profile.max_guests = int(raw.get("personCapacity") or raw.get("maxGuests") or raw.get("guests",0) or 0)
        profile.bedrooms   = int(raw.get("bedrooms",0) or 0)
        profile.beds       = int(raw.get("beds",0) or 0)
        profile.bathrooms  = float(raw.get("bathrooms",0) or 0)
        profile.name       = raw.get("name","")
        profile.rating     = float(raw.get("starRating") or raw.get("rating",0) or 0)
        profile.review_count = int(raw.get("reviewsCount") or raw.get("reviewCount",0) or 0)
        profile.is_superhost = bool(raw.get("isSuperHost") or raw.get("is_superhost",False))

        if not profile.extracted_via:
            profile.extracted_via = "regex+dom"

    except ImportError:
        profile.extracted_via = "unavailable"
    except Exception as e:
        import logging; logging.getLogger(__name__).warning(f"Stage B failed: {e}")
        profile.extracted_via = "failed"

    return profile


# ══════════════════════════════════════════════════════════════
# PATCH 2 — Stage D: Configurable pages + smarter stopping
# ══════════════════════════════════════════════════════════════

def stage_d_retrieve_candidates_patched(
    zones, listing, req, post_graphql_fn, build_body_fn, parse_node_fn,
    min_pages: int = 3,
    max_pages: int = 8,
    target_comps: int = 60,
) -> List:
    """
    PATCH: Smarter candidate retrieval.
    
    Problems fixed:
      - Hard-coded 3 pages → configurable, with auto-stop
      - Stops early if: (a) page returns <9 listings, (b) target_comps reached
      - Deduplicates across zones
    """
    import logging
    log = logging.getLogger(__name__)
    log.info("Stage D: Retrieving candidate listings (patched)…")

    all_comps = []
    seen_ids = set()

    for zone in zones:
        log.info(f"  Zone '{zone.label}'…")

        for page in range(max_pages):
            if len(all_comps) >= target_comps and page >= min_pages:
                log.info(f"  Target {target_comps} comps reached — stopping zone")
                break

            offset = page * 18
            body = build_body_fn(zone, listing, req, offset)
            response = post_graphql_fn(body)

            if not response:
                log.warning(f"  No response page {page+1} — stopping zone")
                break

            nodes = response
            # Walk response tree
            for path in [
                ("data","presentation","staysSearch","results","searchResults"),
                ("data","staysSearch","searchResults"),
            ]:
                cur = response
                for k in path:
                    try: cur = cur[k]
                    except: cur = None; break
                if cur is not None:
                    nodes = cur
                    break

            if not isinstance(nodes, list):
                nodes = []

            page_new = 0
            for node in nodes:
                comp = parse_node_fn(node, listing.latitude, listing.longitude, req.currency)
                if comp and comp.price_per_night > 0 and comp.id not in seen_ids:
                    seen_ids.add(comp.id)
                    all_comps.append(comp)
                    page_new += 1

            log.info(f"    page {page+1}: {page_new} new ({len(nodes)} returned, total {len(all_comps)})")

            # Auto-stop: Airbnb returned fewer than half a page
            if len(nodes) < 9:
                log.info(f"  Sparse page ({len(nodes)} results) — stopping zone")
                break

            if page < max_pages - 1:
                time.sleep(random.uniform(1.0, 2.5))

    log.info(f"  Total candidates: {len(all_comps)}")
    return all_comps


# ══════════════════════════════════════════════════════════════
# PATCH 3 — Stage D+: Date-specific price sampling
# ══════════════════════════════════════════════════════════════

@dataclass
class DatePriceSample:
    """Actual market prices on a specific date."""
    date: str
    checkout: str
    day_type: str          # tuesday / thursday / friday / saturday / holiday
    month: int
    year: int
    prices: List[float]    # all comp prices on this date
    p25: float = 0.0
    p50: float = 0.0
    p75: float = 0.0
    p90: float = 0.0
    sample_count: int = 0
    multiplier_vs_base: float = 1.0


def get_stratified_sample_dates(
    target_months: List[int],
    year: int,
    today: Optional[datetime.date] = None,
) -> List[Dict]:
    """
    ADDED: Stratified date sampling.
    For each month: Tuesday, Thursday, Friday, Saturday + holidays.
    Picks from mid-month to avoid edge effects.
    """
    VN_HOLIDAYS = {(1,1),(4,30),(5,1),(9,2),(1,27),(1,28),(1,29),(1,30),(1,31),
                   (2,16),(2,17),(2,18),(2,19),(2,20)}
    today = today or datetime.date.today()
    samples = []

    day_targets = [
        (1, "tuesday"),
        (3, "thursday"),
        (4, "friday"),
        (5, "saturday"),
    ]

    for month in target_months:
        days_in_month = calendar.monthrange(year, month)[1]
        all_days = [
            datetime.date(year, month, d)
            for d in range(1, days_in_month + 1)
            if datetime.date(year, month, d) > today
        ]
        if not all_days:
            continue

        # Group by weekday
        by_weekday: Dict[int, List] = {i: [] for i in range(7)}
        for d in all_days:
            by_weekday[d.weekday()].append(d)

        # Stratified picks
        for wd, label in day_targets:
            pool = by_weekday[wd]
            if pool:
                mid = len(pool) // 2
                picked = pool[mid]
                samples.append({
                    "date":     str(picked),
                    "checkout": str(picked + datetime.timedelta(days=1)),
                    "day_type": label,
                    "month":    month,
                    "year":     year,
                })

        # Holidays
        for d in all_days:
            if (d.month, d.day) in VN_HOLIDAYS:
                samples.append({
                    "date":     str(d),
                    "checkout": str(d + datetime.timedelta(days=1)),
                    "day_type": "holiday",
                    "month":    month,
                    "year":     year,
                })

    return samples


def _build_graphql_body_dated(
    zone, listing, req,
    checkin: str, checkout: str,
    build_body_fn,
) -> dict:
    """Build a GraphQL body with specific checkin/checkout dates."""
    # Override the req dates temporarily
    class ReqOverride:
        pass
    r_override = ReqOverride()
    r_override.__dict__.update(req.__dict__)
    r_override.checkin  = checkin
    r_override.checkout = checkout
    return build_body_fn(zone, listing, r_override, 0)


def _percentile_simple(data: List[float], p: float) -> float:
    if not data: return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k)+1, len(s)-1)
    return round(s[lo] + (s[hi] - s[lo]) * (k - lo), 2)


def stage_d_plus_price_sampling(
    zones: List,
    listing,
    req,
    target_months: List[int],
    year: int,
    post_graphql_fn,
    build_body_fn,
    parse_node_fn,
    max_dates: int = 16,
) -> Tuple[List[DatePriceSample], Dict]:
    """
    NEW STAGE: Sample actual market prices on specific dates.
    
    For each sample date → call GraphQL with checkin/checkout →
    collect prices → compute distribution.
    
    Returns:
        - samples: List[DatePriceSample] per date
        - summary: aggregated stats by day_type and month
    """
    import logging
    log = logging.getLogger(__name__)
    log.info("Stage D+: Date-specific price sampling…")

    sample_dates = get_stratified_sample_dates(target_months, year)
    if not sample_dates:
        log.warning("  No future sample dates found")
        return [], {}

    # Limit to avoid too many API calls
    sample_dates = sample_dates[:max_dates]
    log.info(f"  Sampling {len(sample_dates)} dates across {len(target_months)} months")

    zone = zones[0]  # Use primary zone for sampling
    results: List[DatePriceSample] = []
    base_prices_collected: List[float] = []

    for sd in sample_dates:
        log.info(f"  Sampling {sd['date']} ({sd['day_type']})…")
        body = _build_graphql_body_dated(zone, listing, req, sd["date"], sd["checkout"], build_body_fn)
        response = post_graphql_fn(body)

        if not response:
            log.warning(f"    No response for {sd['date']}")
            time.sleep(random.uniform(1.5, 3.0))
            continue

        # Extract prices from this response
        nodes = []
        for path in [
            ("data","presentation","staysSearch","results","searchResults"),
            ("data","staysSearch","searchResults"),
        ]:
            cur = response
            for k in path:
                try: cur = cur[k]
                except: cur = None; break
            if cur is not None:
                nodes = cur
                break

        prices = []
        for node in (nodes or []):
            comp = parse_node_fn(node, listing.latitude, listing.longitude, req.currency)
            if comp and comp.price_per_night > 0:
                prices.append(comp.price_per_night)

        if not prices:
            log.warning(f"    No prices for {sd['date']}")
            time.sleep(random.uniform(1.5, 3.0))
            continue

        p50 = _percentile_simple(prices, 50)
        if base_prices_collected:
            base_p50 = _percentile_simple(base_prices_collected, 50)
            multiplier = round(p50 / base_p50, 3) if base_p50 else 1.0
        else:
            multiplier = 1.0

        sample = DatePriceSample(
            date=sd["date"],
            checkout=sd["checkout"],
            day_type=sd["day_type"],
            month=sd["month"],
            year=sd["year"],
            prices=prices,
            p25=_percentile_simple(prices, 25),
            p50=p50,
            p75=_percentile_simple(prices, 75),
            p90=_percentile_simple(prices, 90),
            sample_count=len(prices),
            multiplier_vs_base=multiplier,
        )
        results.append(sample)
        base_prices_collected.extend(prices)

        log.info(f"    {len(prices)} prices: p50={p50:.0f} p75={sample.p75:.0f} mult={multiplier:.2f}")
        time.sleep(random.uniform(1.0, 2.0))

    # ── Aggregate summary ──
    summary: Dict[str, Any] = {}

    # By day_type
    by_type: Dict[str, List[float]] = {}
    for s in results:
        by_type.setdefault(s.day_type, []).extend(s.prices)

    summary["by_day_type"] = {}
    for dt, prices in by_type.items():
        summary["by_day_type"][dt] = {
            "p25": _percentile_simple(prices, 25),
            "p50": _percentile_simple(prices, 50),
            "p75": _percentile_simple(prices, 75),
            "count": len(prices),
        }

    # By month
    by_month: Dict[int, List[float]] = {}
    for s in results:
        by_month.setdefault(s.month, []).extend(s.prices)

    summary["by_month"] = {}
    month_names = ["","Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    for m, prices in by_month.items():
        summary["by_month"][month_names[m]] = {
            "p25": _percentile_simple(prices, 25),
            "p50": _percentile_simple(prices, 50),
            "p75": _percentile_simple(prices, 75),
            "count": len(prices),
        }

    # Weekend premium
    wd_prices = by_type.get("tuesday",[]) + by_type.get("thursday",[])
    we_prices = by_type.get("friday",[]) + by_type.get("saturday",[])
    if wd_prices and we_prices:
        wd_p50 = _percentile_simple(wd_prices, 50)
        we_p50 = _percentile_simple(we_prices, 50)
        summary["weekend_premium_pct"] = round((we_p50/wd_p50 - 1)*100, 1) if wd_p50 else 0

    log.info(f"  Sampling complete: {len(results)} dates, summary by {list(summary.keys())}")
    return results, summary


# ══════════════════════════════════════════════════════════════
# PATCH 4 — Stage G: User-selectable target months
# ══════════════════════════════════════════════════════════════

SEASON_BY_MONTH_DANANG = {
    1:"low", 2:"low", 3:"high", 4:"high", 5:"high",
    6:"peak", 7:"peak", 8:"peak", 9:"high", 10:"medium",
    11:"low", 12:"low",
}

DEMAND_MULTIPLIERS_DETAILED = {
    "peak":   {"tuesday":1.35, "thursday":1.40, "friday":1.60, "saturday":1.70, "sunday":1.50, "weekday":1.40, "weekend":1.65, "holiday":1.85},
    "high":   {"tuesday":1.08, "thursday":1.12, "friday":1.30, "saturday":1.38, "sunday":1.22, "weekday":1.10, "weekend":1.35, "holiday":1.50},
    "medium": {"tuesday":0.95, "thursday":0.97, "friday":1.15, "saturday":1.22, "sunday":1.08, "weekday":0.96, "weekend":1.18, "holiday":1.30},
    "low":    {"tuesday":0.75, "thursday":0.78, "friday":0.88, "saturday":0.95, "sunday":0.82, "weekday":0.76, "weekend":0.90, "holiday":1.10},
}


def stage_g_monthly_strategy_patched(
    pricing,
    listing,
    req,
    target_months: Optional[List[int]] = None,
    target_year: int = 0,
    date_price_summary: Optional[Dict] = None,
) -> List:
    """
    PATCH: Monthly strategy with user-selectable months and date-sampled prices.
    
    target_months: [4, 5, 6] for Apr/May/Jun. If None, uses req.months_ahead.
    date_price_summary: output from stage_d_plus_price_sampling (enriches rates).
    """
    import logging
    log = logging.getLogger(__name__)
    log.info("Stage G: Generating monthly strategy (patched)…")

    today = datetime.date.today()
    year  = target_year or today.year
    base  = pricing.suggested_base
    city  = (listing.city or listing.state or "").lower()

    # Determine months to plan
    if target_months:
        months_to_plan = target_months
        log.info(f"  User-selected months: {months_to_plan} ({year})")
    else:
        months_to_plan = []
        for i in range(req.months_ahead):
            d = today + datetime.timedelta(days=30 * i)
            months_to_plan.append(d.month)
        year = today.year
        log.info(f"  Auto months: {months_to_plan} ({year})")

    # Choose seasonality model
    use_danang = any(x in city for x in ["da nang","danang","đà nẵng","sơn trà","hội an","hoi an","ngũ hành"])

    plans = []
    month_names = ["","January","February","March","April","May","June",
                   "July","August","September","October","November","December"]

    for month in months_to_plan:
        if use_danang:
            season = SEASON_BY_MONTH_DANANG.get(month, "medium")
        else:
            season = "high" if month in {6,7,8,12} else "medium" if month in {4,5,9,10} else "low"

        mults = DEMAND_MULTIPLIERS_DETAILED[season]

        # If we have real sampled prices for this month, use them to calibrate
        mn = month_names[month][:3]  # "Apr"
        real_data = (date_price_summary or {}).get("by_month", {}).get(mn) if date_price_summary else None

        if real_data and real_data.get("p50"):
            # Calibrate base to real market data for this month
            market_p50 = real_data["p50"]
            calibrated_base = market_p50  # use real market median as base
            log.info(f"  {mn}: calibrated to real p50={market_p50:.0f}")
        else:
            calibrated_base = base

        # Day-type rates
        tuesday_rate  = round(calibrated_base * mults["tuesday"], 2)
        thursday_rate = round(calibrated_base * mults["thursday"], 2)
        friday_rate   = round(calibrated_base * mults["friday"], 2)
        saturday_rate = round(calibrated_base * mults["saturday"], 2)
        sunday_rate   = round(calibrated_base * mults["sunday"], 2)
        weekday_rate  = round(calibrated_base * mults["weekday"], 2)
        weekend_rate  = round(calibrated_base * mults["weekend"], 2)
        holiday_rate  = round(calibrated_base * mults["holiday"], 2)
        floor_rate    = round(calibrated_base * 0.68, 2)

        # If real sampled weekend premium available, use it
        real_we_prem  = (date_price_summary or {}).get("weekend_premium_pct")
        if real_we_prem:
            actual_we_rate = round(weekday_rate * (1 + real_we_prem/100), 2)
            saturday_rate  = actual_we_rate
            friday_rate    = round(weekday_rate * (1 + real_we_prem * 0.8 / 100), 2)

        weekend_premium = round((weekend_rate / weekday_rate - 1) * 100, 1) if weekday_rate else 0

        # Min stay logic
        if season == "peak":
            min_stay_wd, min_stay_we = 2, 3
        elif season == "high":
            min_stay_wd, min_stay_we = 1, 2
        else:
            min_stay_wd, min_stay_we = 1, 1

        # Occupancy & revenue targets
        occ_target = {"peak":0.85,"high":0.75,"medium":0.65,"low":0.50}[season]
        days_in_m  = calendar.monthrange(year, month)[1]
        avg_rate   = (weekday_rate * 5 + weekend_rate * 2) / 7
        rev_target = round(avg_rate * days_in_m * occ_target, 0)

        # Gap fill strategy
        gap_strategy = ""
        if season == "low":
            gap_strategy = "Offer 1-night minimum Tue–Wed to fill gaps; last-minute -15% from 48h out"
        elif season == "peak":
            gap_strategy = "3-night minimum Fri–Sat; no last-minute discounts; raise if <2 weeks out"
        else:
            gap_strategy = "2-night minimum weekends; -8% last-minute weekdays if unbooked 7d out"

        plans.append({
            "month": month,
            "month_name": month_names[month],
            "year": year,
            "season": season,
            "calibrated_base": calibrated_base,
            "rates": {
                "tuesday":  tuesday_rate,
                "thursday": thursday_rate,
                "friday":   friday_rate,
                "saturday": saturday_rate,
                "sunday":   sunday_rate,
                "weekday":  weekday_rate,
                "weekend":  weekend_rate,
                "holiday":  holiday_rate,
                "floor":    floor_rate,
            },
            "weekend_premium_pct": weekend_premium,
            "min_stay_weekday":    min_stay_wd,
            "min_stay_weekend":    min_stay_we,
            "occupancy_target":    occ_target,
            "revenue_target":      rev_target,
            "gap_strategy":        gap_strategy,
            "real_market_data":    real_data,
            "notes": f"{month_names[month]} {year} — {season.upper()} season",
        })

    log.info(f"  {len(plans)} month plans: {[p['month_name'][:3] for p in plans]}")
    return plans


# ══════════════════════════════════════════════════════════════
# PATCH 5 — Stage I: Claude AI Synthesis
# ══════════════════════════════════════════════════════════════

CLAUDE_STRATEGY_PROMPT = """You are an expert Airbnb revenue strategist analyzing a listing in Vietnam.

## LISTING PROFILE
{listing_json}

## MARKET PRICING STATISTICS
{pricing_json}

## ACTUAL SAMPLED PRICES BY DATE TYPE
{date_prices_json}

## MONTHLY MARKET DATA (real sampled)
{monthly_json}

## COMPETITOR SUMMARY
{comps_json}

## DEMAND SIGNALS
{demand_json}

## STRATEGY GOAL
Goal: {goal}
Current owner rate: {currency} {your_rate}/night

---

Analyze this Airbnb listing and produce a complete revenue strategy.
You must respond ONLY with valid JSON — no markdown, no explanation, no preamble.

{{
  "positioning": {{
    "current_tier": "budget|mid|premium|luxury",
    "optimal_tier": "budget|mid|premium|luxury",
    "gap_pct": -18.5,
    "gap_direction": "underpriced|overpriced|on_target",
    "reasoning": "one sentence explanation"
  }},
  "price_recommendations": {{
    "weekday_base": 62,
    "thursday": 65,
    "friday": 78,
    "saturday": 84,
    "sunday": 72,
    "holiday": 105,
    "floor": 45
  }},
  "monthly_strategy": [
    {{
      "month": "April 2026",
      "season": "high",
      "weekday_rate": 65,
      "weekend_rate": 82,
      "floor_rate": 45,
      "holiday_rate": 95,
      "strategy_focus": "volume|balanced|revenue",
      "min_stay_weekday": 1,
      "min_stay_weekend": 2,
      "gap_fill_tactic": "specific tactic for empty nights",
      "key_action": "most important thing to do this month",
      "expected_occupancy": 0.72,
      "expected_revenue": 1400
    }}
  ],
  "opportunities": [
    {{
      "type": "underpriced_weekends|supply_gap|review_leverage|superhost_path|low_season_fill",
      "priority": "high|medium|low",
      "revenue_impact": "+X% or +$Y/month",
      "action": "specific action to take"
    }}
  ],
  "listing_improvements": [
    {{
      "area": "photos|title|amenities|pricing_structure|min_stay|description",
      "priority": "high|medium|low",
      "suggestion": "specific suggestion"
    }}
  ],
  "price_sensitivity": {{
    "raise_5pct": "estimated occupancy impact and revenue impact",
    "lower_5pct": "estimated occupancy impact and revenue impact",
    "recommendation": "raise|lower|hold",
    "reasoning": "one sentence"
  }},
  "revenue_projection": {{
    "current_monthly_estimate": 1100,
    "optimized_monthly_estimate": 1680,
    "uplift_pct": 53,
    "key_driver": "what drives the biggest uplift"
  }},
  "confidence": "high|medium|low",
  "confidence_reasoning": "why this confidence level",
  "key_risks": ["risk 1", "risk 2"]
}}"""


def stage_i_claude_synthesis(
    listing,
    comps: List,
    pricing,
    monthly_plans: List[Dict],
    date_price_summary: Dict,
    req,
) -> Dict:
    """
    PATCH: Replace heuristic recommendations with Claude AI synthesis.
    
    Sends structured JSON data to Claude and gets back a complete
    actionable strategy in structured JSON format.
    """
    import logging
    log = logging.getLogger(__name__)
    log.info("Stage I: Claude AI synthesis…")

    # ── Build comp summary ──
    direct_comps  = [c for c in comps if c.tier.value == "direct"][:8]
    top_comps_data = [{
        "name":    c.name[:40],
        "price":   c.price_per_night,
        "rating":  c.rating,
        "reviews": c.review_count,
        "dist_km": c.distance_km,
        "superhost": c.is_superhost,
        "badge":   c.badge,
        "similarity": c.similarity_score,
    } for c in direct_comps]

    # ── Build listing JSON ──
    listing_data = {
        "name":          listing.name,
        "room_type":     listing.room_type,
        "bedrooms":      listing.bedrooms,
        "beds":          listing.beds,
        "bathrooms":     listing.bathrooms,
        "max_guests":    listing.max_guests,
        "city":          listing.city or listing.state,
        "rating":        listing.rating,
        "review_count":  listing.review_count,
        "is_superhost":  listing.is_superhost,
        "badge":         listing.badge,
    }

    # ── Pricing stats ──
    pricing_data = {
        "currency":        pricing.currency,
        "p10": pricing.p10, "p25": pricing.p25,
        "median": pricing.median,
        "p75": pricing.p75, "p90": pricing.p90,
        "weighted_avg":    pricing.weighted_avg,
        "direct_comp_count": pricing.direct_count,
        "suggested_base":  pricing.suggested_base,
        "tier_position":   pricing.tier_position,
    }

    # ── Demand signals ──
    avg_reviews    = sum(c.review_count for c in comps) / len(comps) if comps else 0
    superhost_pct  = sum(1 for c in comps if c.is_superhost) / len(comps) if comps else 0
    high_demand_n  = sum(1 for c in comps if c.review_count > 40)
    demand_data    = {
        "avg_reviews_in_market":  round(avg_reviews, 1),
        "superhost_pct":          round(superhost_pct, 2),
        "high_demand_comps":      high_demand_n,
        "your_estimated_occupancy": 0.45 if listing.review_count < 10 else 0.60,
    }

    # ── Monthly summary ──
    monthly_summary = [{
        "month": p.get("month_name",""), "season": p.get("season",""),
        "real_market_p50": (p.get("real_market_data") or {}).get("p50"),
        "weekday_rate": p.get("rates",{}).get("weekday"),
        "weekend_rate": p.get("rates",{}).get("weekend"),
        "floor_rate":   p.get("rates",{}).get("floor"),
    } for p in monthly_plans]

    prompt = CLAUDE_STRATEGY_PROMPT.format(
        listing_json    = json.dumps(listing_data, ensure_ascii=False),
        pricing_json    = json.dumps(pricing_data, ensure_ascii=False),
        date_prices_json= json.dumps(date_price_summary.get("by_day_type", {}), ensure_ascii=False),
        monthly_json    = json.dumps(date_price_summary.get("by_month", {}), ensure_ascii=False),
        comps_json      = json.dumps(top_comps_data, ensure_ascii=False),
        demand_json     = json.dumps(demand_data, ensure_ascii=False),
        goal            = req.goal.value,
        currency        = req.currency,
        your_rate       = req.your_rate or pricing.suggested_base,
    )

    # ── Call Claude API ──
    try:
        api_body = json.dumps({
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 2000,
            "messages": [{"role": "user", "content": prompt}],
        }, ensure_ascii=False).encode("utf-8")

        api_req = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=api_body,
            headers={
                "Content-Type":      "application/json",
                "anthropic-version": "2023-06-01",
            },
            method="POST",
        )
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(api_req, timeout=60, context=ctx) as resp:
            response_data = json.loads(resp.read().decode("utf-8"))

        raw_text = ""
        for block in response_data.get("content", []):
            if block.get("type") == "text":
                raw_text += block.get("text", "")

        # Strip markdown fences if present
        raw_text = re.sub(r"```(?:json)?", "", raw_text).strip()

        strategy = json.loads(raw_text)
        log.info(f"  Claude synthesis complete — confidence: {strategy.get('confidence','?')}")
        return strategy

    except urllib.error.HTTPError as e:
        log.warning(f"  Claude API HTTP {e.code} — falling back to heuristic")
        return _heuristic_strategy_fallback(pricing, monthly_plans, comps, req)
    except json.JSONDecodeError as e:
        log.warning(f"  Claude JSON parse error: {e} — falling back to heuristic")
        return _heuristic_strategy_fallback(pricing, monthly_plans, comps, req)
    except Exception as e:
        log.warning(f"  Claude synthesis failed: {e} — falling back to heuristic")
        return _heuristic_strategy_fallback(pricing, monthly_plans, comps, req)


def _heuristic_strategy_fallback(pricing, monthly_plans, comps, req) -> Dict:
    """Fallback when Claude API unavailable — heuristic recommendations."""
    your = req.your_rate or pricing.suggested_base
    vs_market_pct = (your - pricing.median) / pricing.median * 100 if pricing.median else 0

    recs = []
    if vs_market_pct < -15:
        recs.append(f"Raise rate: you are {abs(vs_market_pct):.0f}% below market median")
    elif vs_market_pct > 20:
        recs.append(f"Monitor occupancy: you are {vs_market_pct:.0f}% above market median")
    else:
        recs.append("Rate is within market range — focus on conversion (photos, reviews)")

    avg_wprem = sum((p.get("weekend_premium_pct") or 0) for p in monthly_plans) / len(monthly_plans) if monthly_plans else 20
    recs.append(f"Apply {avg_wprem:.0f}% weekend premium")
    recs.append(f"Set floor at {req.currency} {pricing.p25:.0f}/night")

    return {
        "positioning": {
            "current_tier": pricing.tier_position,
            "gap_pct": round(vs_market_pct, 1),
            "gap_direction": "underpriced" if vs_market_pct < 0 else "overpriced" if vs_market_pct > 15 else "on_target",
        },
        "price_recommendations": {
            "weekday_base": pricing.suggested_base,
            "friday":  round(pricing.suggested_base * 1.25, 0),
            "saturday":round(pricing.suggested_base * 1.35, 0),
            "floor":   pricing.p25,
        },
        "opportunities": [{"type": "market_alignment", "priority": "high", "action": r} for r in recs],
        "confidence": "low",
        "confidence_reasoning": "Claude API unavailable — heuristic fallback",
        "note": "Run again with ANTHROPIC_API_KEY set for full AI analysis",
    }


# ══════════════════════════════════════════════════════════════
# INTEGRATION: Full patched run_pipeline
# ══════════════════════════════════════════════════════════════

def run_pipeline_patched(
    listing_url: str,
    radius_km: float = 3.0,
    similarity: str = "medium",
    target_months: Optional[List[int]] = None,
    target_year: int = 0,
    currency: str = "USD",
    goal: str = "balanced",
    your_rate: float = 0.0,
    max_pages: int = 5,
    target_comps: int = 60,
    out_dir=None,
):
    """
    Self-contained patched pipeline.
    Works whether patches are in a separate file OR merged into airbnb_strategy_pipeline.py.
    Resolves all functions from the current module's globals — no cross-file imports.
    """
    import logging
    from pathlib import Path
    log = logging.getLogger(__name__)

    # ── Resolve functions from current module (self-contained) ──
    # Works when merged into one file OR when imported as a module
    import sys
    _this_module = sys.modules[__name__]

    def _get(name):
        """Get function from this module, with clear error if missing."""
        fn = getattr(_this_module, name, None)
        if fn is None:
            raise AttributeError(
                f"Function '{name}' not found in module '{__name__}'.\n"
                f"Make sure all pipeline stages are defined in the same file."
            )
        return fn

    # Resolve all needed functions once
    stage_a_normalize              = _get("stage_a_normalize")
    ListingProfile                 = _get("ListingProfile")
    _dig                           = _get("_dig")
    stage_c_geo_scope              = _get("stage_c_geo_scope")
    stage_e_similarity             = _get("stage_e_similarity")
    stage_f_price_intelligence     = _get("stage_f_price_intelligence")
    stage_h_sample_days            = _get("stage_h_sample_days")
    _post_graphql                  = _get("_post_graphql")
    _build_graphql_body            = _get("_build_graphql_body")
    _parse_node                    = _get("_parse_node")

    log.info("=" * 55)
    log.info("  Airbnb Revenue Strategy Pipeline (PATCHED v2)")
    log.info("=" * 55)

    # A — Input normalization
    months_ahead = len(target_months) if target_months else 3
    req = stage_a_normalize(listing_url, radius_km, similarity, months_ahead, currency, goal, your_rate)

    # B — Listing extraction (PATCHED: 6-strategy spec extraction)
    listing = stage_b_extract_listing_patched(req, ListingProfile, _dig)
    if not listing.latitude:
        log.warning("No coords — using Da Nang defaults")
        listing.latitude, listing.longitude = 16.06147, 108.24408
        listing.city = "Da Nang"
    if your_rate and not listing.nightly_rate:
        listing.nightly_rate = your_rate
        req.your_rate = your_rate

    log.info(
        f"  Extracted: '{listing.name[:40]}' | "
        f"{listing.bedrooms}BR {listing.beds}beds {listing.bathrooms}baths "
        f"{listing.max_guests}guests | {listing.rating}★ {listing.review_count}rev"
    )

    # C — Geo scope
    zones = stage_c_geo_scope(listing, req)

    # D — Candidate retrieval (PATCHED: configurable pages + smart stopping)
    comps = stage_d_retrieve_candidates_patched(
        zones, listing, req,
        _post_graphql, _build_graphql_body, _parse_node,
        min_pages=3, max_pages=max_pages, target_comps=target_comps,
    )
    if not comps:
        raise RuntimeError("No competitors found — check API key/hash or try a larger radius")

    # D+ — Date-specific price sampling (NEW: real prices per sample date)
    today = datetime.date.today()
    year  = target_year or today.year
    months_to_sample = target_months or [
        (today + datetime.timedelta(days=30 * i)).month
        for i in range(months_ahead)
    ]

    date_samples, date_summary = stage_d_plus_price_sampling(
        zones, listing, req,
        target_months=months_to_sample,
        year=year,
        post_graphql_fn=_post_graphql,
        build_body_fn=_build_graphql_body,
        parse_node_fn=_parse_node,
        max_dates=min(16, len(months_to_sample) * 5),
    )

    # E — Similarity scoring
    comps = stage_e_similarity(comps, listing, req)

    # F — Price intelligence
    pricing = stage_f_price_intelligence(comps, listing, req)

    # G — Monthly strategy (PATCHED: user months + real sampled prices)
    monthly_plans = stage_g_monthly_strategy_patched(
        pricing, listing, req,
        target_months=target_months,
        target_year=year,
        date_price_summary=date_summary,
    )

    # H — Sample days (using proxy objects for compatibility with original stage_h)
    class _MonthProxy:
        def __init__(self, d):
            self.__dict__.update(d)
            rates = d.get("rates", {})
            self.base_rate    = d.get("calibrated_base", rates.get("weekday", 0))
            self.weekday_rate = rates.get("weekday", 0)
            self.weekend_rate = rates.get("weekend", 0)
            self.peak_rate    = rates.get("holiday", 0)
            self.floor_rate   = rates.get("floor", 0)
            self.season       = d.get("season", "medium")

    mp_proxies   = [_MonthProxy(p) for p in monthly_plans]
    sample_days  = stage_h_sample_days(pricing, mp_proxies, listing)

    # I — Claude AI synthesis (PATCHED: structured JSON strategy)
    ai_strategy = stage_i_claude_synthesis(
        listing, comps, pricing, monthly_plans, date_summary, req
    )

    # ── Build final result dict ──
    log.info("=" * 55)
    log.info("  STRATEGY COMPLETE")
    log.info("=" * 55)
    p = pricing
    log.info(f"  Listing  : {listing.name[:45]}")
    log.info(f"  Comps    : {p.comp_count} ({p.direct_count} direct / {p.secondary_count} sec / {p.context_count} ctx)")
    log.info(f"  Market   : {p.currency} {p.median:.0f} median  band [{p.p25:.0f}–{p.p75:.0f}]")
    log.info(f"  Suggested: {p.currency} {p.suggested_base:.0f}")
    log.info(f"  AI conf  : {ai_strategy.get('confidence', '?')}")
    log.info(f"  Months   : {[pl.get('month_name','')[:3] for pl in monthly_plans]}")

    result = {
        "listing":       listing.__dict__,
        "pricing":       pricing.__dict__,
        "monthly_plans": monthly_plans,
        "date_samples":  [s.__dict__ for s in date_samples],
        "date_summary":  date_summary,
        "sample_days":   [s.__dict__ for s in sample_days],
        "comps":         [c.__dict__ for c in comps[:30]],
        "ai_strategy":   ai_strategy,
        "confidence":    ai_strategy.get("confidence", "medium"),
        "generated_at":  datetime.datetime.now().isoformat(),
        "request": {
            "url":            listing_url,
            "radius_km":      radius_km,
            "target_months":  months_to_sample,
            "year":           year,
            "currency":       currency,
            "goal":           goal,
            "your_rate":      your_rate,
        },
    }

    if out_dir:
        out_path = Path(out_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        # strategy_full.json
        (out_path / "strategy_full.json").write_text(
            json.dumps(result, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        log.info(f"  Saved → {out_path / 'strategy_full.json'}")

        # comps.csv
        generate_comps_csv(result, out_path / "comps.csv")

        # price_calendar.csv
        cal = generate_price_calendar(result, out_path / "price_calendar.csv")

        # strategy_report.txt
        generate_strategy_report(result, cal, out_path / "strategy_report.txt")

        # claude_prompt.txt
        generate_claude_prompt(result, out_path / "claude_prompt.txt")

        log.info(f"  ✓ All 5 outputs saved → {out_path}")

    return result




# ──────────────────────────────────────────────────────────────
# OUTPUT GENERATORS
# ──────────────────────────────────────────────────────────────

def _tier_str(t) -> str:
    """Normalise CompTier to plain string."""
    if isinstance(t, dict): return t.get('value', '?')
    s = str(t)
    return s.split('.')[-1].lower()


def _percentile_v2(data: list, p: float) -> float:
    if not data: return 0.0
    s = sorted(data)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k)+1, len(s)-1)
    return round(s[lo] + (s[hi]-s[lo])*(k-lo), 2)


def generate_comps_csv(result: dict, out_path) -> None:
    """Write comps.csv with all competitor data."""
    import csv, io
    comps = result.get('comps', [])
    if not comps:
        log.warning("No comps to write")
        return

    fields = [
        "rank","id","name","price_per_night","currency",
        "bedrooms","beds","rating","review_count",
        "distance_km","is_superhost","badge",
        "estimated_occupancy","similarity_score","tier","url"
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, extrasaction='ignore')
    writer.writeheader()
    for i, c in enumerate(comps, 1):
        writer.writerow({
            "rank":               i,
            "id":                 c.get('id',''),
            "name":               c.get('name',''),
            "price_per_night":    c.get('price_per_night', 0),
            "currency":           c.get('currency', 'USD'),
            "bedrooms":           c.get('bedrooms', 0),
            "beds":               c.get('beds', 0),
            "rating":             c.get('rating', 0),
            "review_count":       c.get('review_count', 0),
            "distance_km":        round(c.get('distance_km', -1), 2),
            "is_superhost":       c.get('is_superhost', False),
            "badge":              c.get('badge', ''),
            "estimated_occupancy":c.get('estimated_occupancy', 0),
            "similarity_score":   round(c.get('similarity_score', 0), 3),
            "tier":               _tier_str(c.get('tier', '')),
            "url":                c.get('url', ''),
        })
    from pathlib import Path as _Path
    _Path(out_path).write_text(buf.getvalue(), encoding='utf-8')
    log.info(f"  comps.csv → {out_path} ({len(comps)} rows)")


def generate_price_calendar(result: dict, out_path) -> dict:
    """
    Generate a day-by-day price calendar for each month in monthly_plans.
    Returns the calendar dict AND writes price_calendar.csv.
    """
    import csv, io, calendar as _cal
    VN_HOLIDAYS = {(1,1),(4,30),(5,1),(9,2),(1,27),(1,28),(1,29),(1,30),(1,31),
                   (2,16),(2,17),(2,18),(2,19),(2,20)}

    plans = result.get('monthly_plans', [])
    listing = result.get('listing', {})
    cur = result.get('pricing', {}).get('currency', 'USD')
    sym = '$' if cur == 'USD' else '₫'

    calendar_rows = []

    for plan in plans:
        month     = plan.get('month', 0)
        year      = plan.get('year', datetime.date.today().year)
        season    = plan.get('season', 'medium')
        rates     = plan.get('rates', {})
        cal_base  = plan.get('calibrated_base', 0)
        floor_r   = rates.get('floor', cal_base * 0.68)

        days_in_month = _cal.monthrange(year, month)[1]

        for day in range(1, days_in_month + 1):
            d  = datetime.date(year, month, day)
            wd = d.weekday()  # 0=Mon … 6=Sun

            is_holiday = (d.month, d.day) in VN_HOLIDAYS

            if is_holiday:
                price    = rates.get('holiday', cal_base * 1.80)
                demand   = 'holiday'
                day_type = 'holiday'
                min_stay = plan.get('min_stay_weekend', 2)
            elif wd == 0:
                price    = rates.get('weekday', cal_base)
                demand   = 'low'
                day_type = 'monday'
                min_stay = plan.get('min_stay_weekday', 1)
            elif wd == 1:
                price    = rates.get('tuesday', rates.get('weekday', cal_base))
                demand   = 'low'
                day_type = 'tuesday'
                min_stay = plan.get('min_stay_weekday', 1)
            elif wd == 2:
                price    = rates.get('weekday', cal_base)
                demand   = 'low'
                day_type = 'wednesday'
                min_stay = plan.get('min_stay_weekday', 1)
            elif wd == 3:
                price    = rates.get('thursday', rates.get('weekday', cal_base))
                demand   = 'medium'
                day_type = 'thursday'
                min_stay = plan.get('min_stay_weekday', 1)
            elif wd == 4:
                price    = rates.get('friday', rates.get('weekend', cal_base * 1.30))
                demand   = 'high'
                day_type = 'friday'
                min_stay = plan.get('min_stay_weekend', 2)
            elif wd == 5:
                price    = rates.get('saturday', rates.get('weekend', cal_base * 1.35))
                demand   = 'high'
                day_type = 'saturday'
                min_stay = plan.get('min_stay_weekend', 2)
            else:
                price    = rates.get('sunday', rates.get('weekend', cal_base * 1.20))
                demand   = 'medium'
                day_type = 'sunday'
                min_stay = plan.get('min_stay_weekend', 2)

            calendar_rows.append({
                'date':        str(d),
                'day_name':    d.strftime('%A'),
                'day_type':    day_type,
                'month':       plan.get('month_name', ''),
                'season':      season,
                'demand':      demand,
                'price':       round(float(price), 2),
                'floor':       round(float(floor_r), 2),
                'currency':    cur,
                'min_stay':    min_stay,
                'is_holiday':  is_holiday,
                'week_num':    d.isocalendar()[1],
            })

    if not calendar_rows:
        log.warning("No calendar rows generated — no monthly_plans?")
        return {}

    # Write CSV
    buf = io.StringIO()
    fields = ['date','day_name','day_type','month','season','demand',
              'price','floor','currency','min_stay','is_holiday','week_num']
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    writer.writerows(calendar_rows)
    from pathlib import Path as _Path
    _Path(out_path).write_text(buf.getvalue(), encoding='utf-8')
    log.info(f"  price_calendar.csv → {out_path} ({len(calendar_rows)} days)")

    return {r['date']: r for r in calendar_rows}


def generate_strategy_report(result: dict, calendar: dict, out_path) -> None:
    """Write a complete human-readable strategy_report.txt."""
    import statistics as _stats

    listing  = result.get('listing', {})
    pricing  = result.get('pricing', {})
    plans    = result.get('monthly_plans', [])
    samples  = result.get('date_samples', [])
    ds       = result.get('date_summary', {})
    comps    = result.get('comps', [])
    ai       = result.get('ai_strategy', {})
    req      = result.get('request', {})
    cur      = pricing.get('currency', 'USD')
    sym      = '$'
    your_rate = float(listing.get('nightly_rate', 0) or req.get('your_rate', 0))

    # ── Corrected 2BR stats ──
    two_br_prices = sorted([
        c['price_per_night'] for c in comps
        if c.get('bedrooms') == 2 and c.get('price_per_night', 0) > 0
    ])
    true_2br_median = _stats.median(two_br_prices) if two_br_prices else pricing.get('median', 0)
    true_2br_p25    = two_br_prices[max(0, len(two_br_prices)//4-1)] if two_br_prices else pricing.get('p25', 0)
    true_2br_p75    = two_br_prices[min(len(two_br_prices)-1, 3*len(two_br_prices)//4)] if two_br_prices else pricing.get('p75', 0)

    # ── Sample prices ──
    def _sp(day_type):
        s = next((x for x in samples if x.get('day_type') == day_type), None)
        return s.get('p50', 0) if s else 0

    tue_p50 = _sp('tuesday'); thu_p50 = _sp('thursday')
    fri_p50 = _sp('friday');  sat_p50 = _sp('saturday'); hol_p50 = _sp('holiday')
    wd_base = (tue_p50 + thu_p50) / 2 if tue_p50 and thu_p50 else tue_p50 or thu_p50
    we_base = max(fri_p50, sat_p50) if fri_p50 or sat_p50 else 0
    hol_prem = round((hol_p50/wd_base-1)*100, 1) if wd_base else 0
    we_prem  = round((we_base/wd_base-1)*100, 1) if wd_base else 0
    vs_2br   = round((your_rate - true_2br_median) / true_2br_median * 100, 1) if true_2br_median else 0

    sep  = "═" * 64
    sep2 = "─" * 64
    lines = [
        sep,
        "  AIRBNB REVENUE STRATEGY REPORT",
        f"  Generated : {result.get('generated_at','')[:19]}",
        f"  Listing   : {listing.get('url','')}",
        sep, "",
    ]

    # § 1 Listing
    lines += [
        "❶  LISTING PROFILE", sep2,
        f"  Name         : {listing.get('name') or '(not extracted — check URL)'}",
        f"  Room type    : {listing.get('room_type') or 'Entire home (inferred)'}",
        f"  Specs        : {listing.get('bedrooms',0)}BR · {listing.get('beds',0)} beds · {listing.get('bathrooms',0)} baths · {listing.get('max_guests',0)} guests",
        f"  Location     : {listing.get('city','')}  {listing.get('state','')}",
        f"  Coordinates  : {listing.get('latitude','')} , {listing.get('longitude','')}",
        f"  Rating       : {listing.get('rating',0)}★  ({listing.get('review_count',0)} reviews)",
        f"  Superhost    : {'Yes ★' if listing.get('is_superhost') else 'No'}",
        f"  Badge        : {listing.get('badge') or 'None'}",
        f"  Your rate    : {sym}{your_rate:.0f}/night",
        f"  Extracted via: {listing.get('extracted_via','')}",
        "",
    ]
    if not listing.get('rating'):
        lines.append("  ⚠  Rating=0 — Stage B extraction may have missed the reviews page.")
        lines.append("")

    # § 2 Market
    tier_counts = {}
    for c in comps:
        t = _tier_str(c.get('tier',''))
        tier_counts[t] = tier_counts.get(t,0)+1

    lines += [
        "❷  MARKET PRICING ANALYSIS", sep2,
        f"  Pool          : {pricing.get('comp_count',0)} comps total  "
        f"({tier_counts.get('direct',0)} direct / {tier_counts.get('secondary',0)} secondary / {tier_counts.get('context',0)} context)",
        f"  Outliers removed: {pricing.get('outliers_removed',0)}",
        "",
        f"  ── Full pool (all bedrooms) ──",
        f"  P10 {sym}{pricing.get('p10',0):.0f}  │  P25 {sym}{pricing.get('p25',0):.0f}  │  "
        f"Median {sym}{pricing.get('median',0):.0f}  │  P75 {sym}{pricing.get('p75',0):.0f}  │  P90 {sym}{pricing.get('p90',0):.0f}",
        "",
        f"  ── 2BR peer group ({len(two_br_prices)} comps) ──",
        f"  P25 {sym}{true_2br_p25:.0f}  │  Median {sym}{true_2br_median:.0f}  │  P75 {sym}{true_2br_p75:.0f}",
        "",
        f"  Your {sym}{your_rate:.0f} vs 2BR median {sym}{true_2br_median:.0f}  →  {vs_2br:+.0f}%  "
        f"({'UNDERPRICED' if vs_2br < -5 else 'OVERPRICED' if vs_2br > 15 else 'ON TARGET'})",
        f"  Tier: {pricing.get('tier_position','?').upper()}",
        "",
    ]

    # § 3 Sampled prices
    if samples:
        lines += [
            "❸  ACTUAL SAMPLED MARKET PRICES", sep2,
            f"  {'Day type':<14} {'Date':<13} {'P25':>6}  {'P50':>6}  {'P75':>6}  {'Mult':>7}",
            f"  {'-'*14} {'-'*13} {'-'*6}  {'-'*6}  {'-'*6}  {'-'*7}",
        ]
        for s in samples:
            lines.append(
                f"  {s.get('day_type',''):<14} {s.get('date',''):<13} "
                f"{sym}{s.get('p25',0):>5.0f}  {sym}{s.get('p50',0):>5.0f}  "
                f"{sym}{s.get('p75',0):>5.0f}  ×{s.get('multiplier_vs_base',1):>5.3f}"
            )
        lines += [
            "",
            f"  Weekday base (Tue+Thu / 2) : {sym}{wd_base:.0f}",
            f"  Weekend base (max Fri/Sat) : {sym}{we_base:.0f}",
            f"  Weekend premium (sampled)  : {we_prem:+.1f}%",
            f"  Holiday premium vs weekday : {hol_prem:+.1f}%",
            "",
        ]

    # § 4 Monthly plans
    lines += ["❹  MONTHLY STRATEGY", sep2]
    for plan in plans:
        rates  = plan.get('rates', {})
        real   = plan.get('real_market_data') or {}
        lines += [
            f"  {plan.get('month_name','')} {plan.get('year','')}  [{plan.get('season','').upper()}]",
        ]
        if real:
            lines.append(f"  Real market:  P25 {sym}{real.get('p25',0):.0f}  "
                         f"P50 {sym}{real.get('p50',0):.0f}  P75 {sym}{real.get('p75',0):.0f}  "
                         f"(n={real.get('count',0)})")
        lines += [
            f"  {'Day type':<20} {'Price':>8}",
            f"  {'-'*20} {'-'*8}",
            f"  {'Mon–Wed (base)':<20} {sym}{rates.get('weekday',0):>7.0f}",
            f"  {'Thursday':<20} {sym}{rates.get('thursday',0):>7.0f}",
            f"  {'Friday':<20} {sym}{rates.get('friday',0):>7.0f}",
            f"  {'Saturday':<20} {sym}{rates.get('saturday',0):>7.0f}",
            f"  {'Sunday':<20} {sym}{rates.get('sunday',0):>7.0f}",
            f"  {'Holiday':<20} {sym}{rates.get('holiday',0):>7.0f}",
            f"  {'Floor (min)':<20} {sym}{rates.get('floor',0):>7.0f}",
            "",
            f"  Occ target   : {plan.get('occupancy_target',0):.0%}",
            f"  Rev target   : {sym}{plan.get('revenue_target',0):,.0f}/month",
            f"  Min stay wd  : {plan.get('min_stay_weekday',1)} night  "
            f"│  Min stay we : {plan.get('min_stay_weekend',2)} nights",
            f"  Gap tactic   : {plan.get('gap_strategy','')}",
            "",
        ]

    # § 5 Price calendar summary
    if calendar:
        lines += ["❺  PRICE CALENDAR SUMMARY", sep2]
        for plan in plans:
            month = plan.get('month', 0)
            year  = plan.get('year', 0)
            import calendar as _cal2
            month_name = plan.get('month_name', '')
            days_in = _cal2.monthrange(year, month)[1]
            # Build mini calendar display
            lines.append(f"  {month_name} {year}:")
            lines.append(f"  {'Mo':>5} {'Tu':>5} {'We':>5} {'Th':>5} {'Fr':>6} {'Sa':>6} {'Su':>6}")
            import datetime as _dt
            first_wd = _dt.date(year, month, 1).weekday()
            cells = [''] * first_wd
            for d in range(1, days_in+1):
                date_str = f"{year}-{month:02d}-{d:02d}"
                info = calendar.get(date_str, {})
                pr   = info.get('price', 0)
                flag = '🔴' if info.get('is_holiday') else ('🟡' if info.get('demand') == 'high' else '')
                cells.append(f"{sym}{pr:.0f}{flag}")
            # Pad to full weeks
            while len(cells) % 7 != 0:
                cells.append('')
            weeks = [cells[i:i+7] for i in range(0, len(cells), 7)]
            for week in weeks:
                row = '  ' + '  '.join(f"{c:>6}" for c in week)
                lines.append(row)
            lines.append(f"  (full calendar → price_calendar.csv)")
            lines.append("")

    # § 6 Top comps
    lines += ["❻  TOP COMPETITORS", sep2,
              f"  {'#':<3} {'Name':<38} {'$/n':>5} {'⭐':>5} {'Rev':>5} {'km':>5} {'BR':>3} {'Sim':>5} Tier",
              f"  {'-'*3} {'-'*38} {'-'*5} {'-'*5} {'-'*5} {'-'*5} {'-'*3} {'-'*5} {'-'*10}"]
    for i, c in enumerate(comps[:20], 1):
        t    = _tier_str(c.get('tier',''))
        dist = c.get('distance_km', -1)
        lines.append(
            f"  {i:<3} {c.get('name','')[:37]:<38} "
            f"{sym}{c.get('price_per_night',0):>4.0f} "
            f"{c.get('rating',0):>5.2f} "
            f"{c.get('review_count',0):>5} "
            f"{dist:>5.1f} "
            f"{c.get('bedrooms',0):>3} "
            f"{c.get('similarity_score',0):>5.2f} "
            f"{t}"
        )
    lines += ["", f"  Full list → comps.csv", ""]

    # § 7 Recommendations
    lines += ["❼  STRATEGIC RECOMMENDATIONS", sep2]
    pos = ai.get('positioning', {})
    if pos.get('reasoning'):
        lines.append(f"  Position: {pos.get('gap_direction','?').upper()}  {pos.get('gap_pct',0):+.1f}%")
        lines.append(f"  {pos.get('reasoning','')}")
        lines.append("")

    pr = ai.get('price_recommendations', {})
    if pr:
        lines += [
            "  Recommended rates:",
            f"    Weekday base : {sym}{pr.get('weekday_base', 0):.0f}",
            f"    Friday       : {sym}{pr.get('friday', 0):.0f}",
            f"    Saturday     : {sym}{pr.get('saturday', 0):.0f}",
            f"    Holiday      : {sym}{pr.get('holiday', pr.get('peak_event', 0)):.0f}",
            f"    Floor        : {sym}{pr.get('floor', 0):.0f}",
            "",
        ]

    for opp in (ai.get('opportunities') or []):
        pri = opp.get('priority','?').upper()
        lines.append(f"  [{pri}] {opp.get('action','')}")
    lines.append("")

    proj = ai.get('revenue_projection', {})
    if proj:
        lines += [
            "  Revenue projection:",
            f"    Current     : {sym}{proj.get('current_monthly_estimate',0):,}/month",
            f"    Optimized   : {sym}{proj.get('optimized_monthly_estimate',0):,}/month  (+{proj.get('uplift_pct',0)}%)",
            f"    Key driver  : {proj.get('key_driver','')}",
            "",
        ]

    # § 8 Confidence
    lines += [
        "❽  CONFIDENCE & DATA QUALITY", sep2,
        f"  Confidence: {result.get('confidence','?').upper()}",
        f"  {ai.get('confidence_reasoning','')}",
        "",
    ]

    report = "\n".join(lines)
    from pathlib import Path as _Path
    _Path(out_path).write_text(report, encoding='utf-8')
    log.info(f"  strategy_report.txt → {out_path} ({len(lines)} lines)")


CLAUDE_PROMPT_TEMPLATE = """# Phân tích Airbnb Revenue Strategy

Bạn là chuyên gia tối ưu doanh thu Airbnb. Hãy phân tích JSON data bên dưới và đưa ra chiến lược cụ thể, có thể thực hiện ngay.

## Hướng dẫn
- Trả lời bằng **tiếng Việt**, số tiền giữ nguyên USD
- Mọi khuyến nghị phải **cụ thể với con số** — không nói chung chung
- Chỉ ra anomaly trong data nếu có (weekend premium âm, thiếu field, v.v.)
- Tập trung vào 3 hành động có impact cao nhất

## Data
```json
{JSON_PLACEHOLDER}
```

---

## Yêu cầu output

### 1. TÓM TẮT LISTING
- Tên, loại phòng, specs (BR/beds/baths/guests)
- Vị trí, rating, số reviews
- Giá hiện tại và vị trí so với thị trường

### 2. PHÂN TÍCH THỊ TRƯỜNG
| Chỉ số | Giá trị |
|---|---|
| Giá trung vị 2BR (peer group) | $XX |
| P25 | $XX |
| P75 | $XX |
| Giá của bạn | $XX |
| Chênh lệch vs median | XX% |
| Tier hiện tại | budget/mid/premium |

Top 5 đối thủ gần nhất (từ comps array):
- Tên — giá — rating — khoảng cách

### 3. GIÁ ĐỀ XUẤT THEO LOẠI NGÀY
| Loại ngày | Giá đề xuất | Floor | Ghi chú |
|---|---|---|---|
| Thứ 2–3 | $XX | $XX | ... |
| Thứ 4 | $XX | $XX | ... |
| Thứ 5 | $XX | $XX | ... |
| Thứ 6 | $XX | $XX | ... |
| Thứ 7 | $XX | $XX | ... |
| Chủ nhật | $XX | $XX | ... |
| Lễ / Holiday | $XX | $XX | ... |

### 4. CHIẾN LƯỢC TỪNG THÁNG
Với mỗi tháng có trong monthly_plans:

**[Tên tháng] [Năm] — [Mùa]**
- Market thực tế (từ date_summary): P50 = $XX
- Weekday: $XX | Weekend: $XX | Floor: $XX
- Mục tiêu occupancy: XX% → doanh thu ước tính $X,XXX/tháng
- Min stay: X đêm weekday / X đêm weekend
- Hành động chính tháng này: [1 câu cụ thể]

### 5. BA HÀNH ĐỘNG NGAY LẬP TỨC (trong 48 giờ)
1. **[Tên hành động]**: [chi tiết cụ thể với số]
2. **[Tên hành động]**: [chi tiết cụ thể với số]
3. **[Tên hành động]**: [chi tiết cụ thể với số]

### 6. LỘ TRÌNH 90 NGÀY
| Tuần | Mục tiêu | Hành động |
|---|---|---|
| 1–2 | ... | ... |
| 3–4 | ... | ... |
| 5–8 | ... | ... |
| 9–12 | ... | ... |

### 7. DỰ BÁO DOANH THU
| Kịch bản | Giá/đêm | Occ% | Doanh thu/tháng |
|---|---|---|---|
| Hiện tại (giữ nguyên) | $XX | XX% | $X,XXX |
| Ngắn hạn (1–30 ngày) | $XX | XX% | $X,XXX |
| Mục tiêu 90 ngày | $XX | XX% | $X,XXX |

### 8. CẢNH BÁO & ĐỘ TIN CẬY
- Data anomalies: [list cụ thể]
- Độ tin cậy: Cao / Trung bình / Thấp
- Lý do: [giải thích ngắn]
- Cần thêm data gì để tăng độ tin cậy?
"""


def generate_claude_prompt(result: dict, out_path) -> str:
    """
    Write a ready-to-paste Claude prompt with the strategy JSON embedded.
    The prompt is in Vietnamese and instructs Claude to produce a full analysis.
    """
    import json as _json
    from pathlib import Path as _Path

    # Build a trimmed version of result for the prompt
    # (drop raw prices list to save tokens, keep summaries)
    slim = {
        "listing":       result.get('listing', {}),
        "pricing":       result.get('pricing', {}),
        "monthly_plans": result.get('monthly_plans', []),
        "date_summary":  result.get('date_summary', {}),
        "date_samples":  result.get('date_samples', []),
        "comps":         result.get('comps', [])[:15],  # top 15 comps only
        "ai_strategy":   result.get('ai_strategy', {}),
        "request":       result.get('request', {}),
        "confidence":    result.get('confidence', ''),
        "generated_at":  result.get('generated_at', ''),
    }

    json_str = _json.dumps(slim, indent=2, ensure_ascii=False, default=str)
    prompt   = CLAUDE_PROMPT_TEMPLATE.replace("{JSON_PLACEHOLDER}", json_str)

    _Path(out_path).write_text(prompt, encoding='utf-8')
    log.info(f"  claude_prompt.txt → {out_path}  (~{len(prompt)//4} tokens)")
    return prompt


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")

    print("=" * 55)
    print("  Airbnb Pipeline — Patched Runner")
    print("=" * 55)

    url = input("\nAirbnb listing URL: ").strip()
    if not url:
        url = "https://www.airbnb.com/rooms/730105454611509413"
        print(f"  Using demo: {url}")

    months_input = input("Target months e.g. 4,5,6 [Enter=next 3]: ").strip()
    if months_input:
        target_months = [int(m.strip()) for m in months_input.split(",") if m.strip().isdigit()]
    else:
        target_months = None

    year_input = input("Year [2026]: ").strip()
    year = int(year_input) if year_input.isdigit() else 2026

    radius_s  = input("Radius km [3]: ").strip()
    radius_km = float(radius_s) if radius_s else 3.0

    cur_s = input("Currency [USD]: ").strip().upper() or "USD"
    goal_s = input("Goal [revenue/occupancy/balanced]: ").strip() or "balanced"

    rate_s = input(f"Your current rate ({cur_s}): ").strip()
    your_rate = float(rate_s.replace(",","")) if rate_s else 0.0

    from pathlib import Path
    out_dir = Path.home() / "openclaw" / "airbnb" / "strategy"

    result = run_pipeline_patched(
        listing_url=url,
        radius_km=radius_km,
        target_months=target_months,
        target_year=year,
        currency=cur_s,
        goal=goal_s,
        your_rate=your_rate,
        max_pages=5,
        target_comps=60,
        out_dir=out_dir,
    )

    # Print AI strategy summary
    ai = result.get("ai_strategy", {})
    p  = result.get("pricing", {})
    print(f"\n{'='*55}")
    print(f"  RESULT")
    print(f"{'='*55}")
    print(f"  Comps:      {p.get('comp_count',0)} ({p.get('direct_count',0)} direct)")
    print(f"  Market P50: {p.get('currency','$')} {p.get('median',0):.0f}")
    print(f"  Suggested:  {p.get('currency','$')} {p.get('suggested_base',0):.0f}")

    pos = ai.get("positioning", {})
    if pos:
        print(f"  Position:   {pos.get('gap_direction','?')} by {abs(pos.get('gap_pct',0)):.0f}%")

    proj = ai.get("revenue_projection", {})
    if proj:
        print(f"  Revenue:    current ${proj.get('current_monthly_estimate',0):,}/mo → optimized ${proj.get('optimized_monthly_estimate',0):,}/mo (+{proj.get('uplift_pct',0)}%)")

    print(f"  Confidence: {ai.get('confidence','?').upper()}")
    print(f"\n  Top opportunities:")
    for opp in (ai.get("opportunities") or [])[:3]:
        print(f"  • [{opp.get('priority','?').upper()}] {opp.get('action','')}")
    print(f"\n  Full output → {out_dir / 'strategy_full.json'}")