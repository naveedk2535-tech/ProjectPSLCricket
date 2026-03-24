"""
PSL Team Name Standardization.
ALL external data must pass through standardise() before DB storage.
This prevents the #1 silent killer: team name fragmentation.
"""

# Canonical names → all known variations
NAME_MAP = {
    # Islamabad United
    "Islamabad": "Islamabad United",
    "ISU": "Islamabad United",
    "United": "Islamabad United",
    "Islamabad Utd": "Islamabad United",
    "Islam United": "Islamabad United",
    "Islamabad U": "Islamabad United",

    # Karachi Kings
    "Karachi": "Karachi Kings",
    "KK": "Karachi Kings",
    "Kings": "Karachi Kings",
    "Kar Kings": "Karachi Kings",
    "Karachi K": "Karachi Kings",

    # Lahore Qalandars
    "Lahore": "Lahore Qalandars",
    "LQ": "Lahore Qalandars",
    "Qalandars": "Lahore Qalandars",
    "Lahore Qalandar": "Lahore Qalandars",
    "Lahore Q": "Lahore Qalandars",
    "LHQ": "Lahore Qalandars",

    # Multan Sultans
    "Multan": "Multan Sultans",
    "MS": "Multan Sultans",
    "Sultans": "Multan Sultans",
    "Multan S": "Multan Sultans",
    "MUL": "Multan Sultans",

    # Peshawar Zalmi
    "Peshawar": "Peshawar Zalmi",
    "PZ": "Peshawar Zalmi",
    "Zalmi": "Peshawar Zalmi",
    "Peshawar Z": "Peshawar Zalmi",
    "PSZ": "Peshawar Zalmi",

    # Quetta Gladiators
    "Quetta": "Quetta Gladiators",
    "QG": "Quetta Gladiators",
    "Gladiators": "Quetta Gladiators",
    "Quetta G": "Quetta Gladiators",
    "QTG": "Quetta Gladiators",

    # Hyderabad Kingsmen (NEW 2026)
    "Hyderabad": "Hyderabad Kingsmen",
    "HK": "Hyderabad Kingsmen",
    "Kingsmen": "Hyderabad Kingsmen",
    "Hyderabad K": "Hyderabad Kingsmen",
    "HYD": "Hyderabad Kingsmen",
    "Hyderabad Houston Kingsmen": "Hyderabad Kingsmen",

    # Rawalpindi Pindiz (NEW 2026)
    "Rawalpindi": "Rawalpindi Pindiz",
    "RP": "Rawalpindi Pindiz",
    "Pindiz": "Rawalpindi Pindiz",
    "Rawalpindi P": "Rawalpindi Pindiz",
    "RWP": "Rawalpindi Pindiz",
    "Pindi": "Rawalpindi Pindiz",
}

# Canonical team names
CANONICAL_TEAMS = [
    "Islamabad United",
    "Karachi Kings",
    "Lahore Qalandars",
    "Multan Sultans",
    "Peshawar Zalmi",
    "Quetta Gladiators",
    "Hyderabad Kingsmen",
    "Rawalpindi Pindiz",
]

# Team abbreviations
ABBREVIATIONS = {
    "Islamabad United": "ISU",
    "Karachi Kings": "KK",
    "Lahore Qalandars": "LQ",
    "Multan Sultans": "MS",
    "Peshawar Zalmi": "PZ",
    "Quetta Gladiators": "QG",
    "Hyderabad Kingsmen": "HK",
    "Rawalpindi Pindiz": "RP",
}

# Venue name standardization
VENUE_MAP = {
    "Gaddafi Stadium, Lahore": "Gaddafi Stadium",
    "Gaddafi Stadium Lahore": "Gaddafi Stadium",
    "Lahore": "Gaddafi Stadium",
    "National Stadium, Karachi": "National Stadium",
    "National Stadium Karachi": "National Stadium",
    "Karachi": "National Stadium",
    "Rawalpindi Cricket Stadium": "Rawalpindi Cricket Stadium",
    "Pindi Cricket Stadium": "Rawalpindi Cricket Stadium",
    "Rawalpindi Stadium": "Rawalpindi Cricket Stadium",
    "Multan Cricket Stadium": "Multan Cricket Stadium",
    "Multan Stadium": "Multan Cricket Stadium",
    "Iqbal Stadium, Faisalabad": "Iqbal Stadium",
    "Iqbal Stadium Faisalabad": "Iqbal Stadium",
    "Faisalabad": "Iqbal Stadium",
    "Arbab Niaz Stadium, Peshawar": "Arbab Niaz Stadium",
    "Arbab Niaz Stadium Peshawar": "Arbab Niaz Stadium",
    "Peshawar": "Arbab Niaz Stadium",
}


def standardise(name):
    """Convert any team name variation to canonical form."""
    if not name:
        return name

    name = name.strip()

    # Already canonical
    if name in CANONICAL_TEAMS:
        return name

    # Direct lookup
    if name in NAME_MAP:
        return NAME_MAP[name]

    # Case-insensitive lookup
    name_lower = name.lower()
    for variant, canonical in NAME_MAP.items():
        if variant.lower() == name_lower:
            return canonical

    # Fuzzy: check if any canonical name is contained in the input
    for canonical in CANONICAL_TEAMS:
        if canonical.lower() in name_lower or name_lower in canonical.lower():
            return canonical

    return name


def standardise_venue(venue):
    """Convert any venue name variation to canonical form."""
    if not venue:
        return venue

    venue = venue.strip()

    if venue in VENUE_MAP:
        return VENUE_MAP[venue]

    venue_lower = venue.lower()
    for variant, canonical in VENUE_MAP.items():
        if variant.lower() == venue_lower:
            return canonical

    # Check if any canonical venue name is contained
    canonical_venues = set(VENUE_MAP.values())
    for cv in canonical_venues:
        if cv.lower() in venue_lower:
            return cv

    return venue


def get_all_teams():
    """Return list of canonical team names."""
    return CANONICAL_TEAMS.copy()


def get_abbreviation(name):
    """Get team abbreviation from canonical or any name."""
    canonical = standardise(name)
    return ABBREVIATIONS.get(canonical, name[:3].upper())


def get_team_color(name):
    """Get team color for UI display."""
    canonical = standardise(name)
    abbrev = ABBREVIATIONS.get(canonical)
    try:
        import config as _cfg
        if abbrev and abbrev in _cfg.TEAMS:
            return _cfg.TEAMS[abbrev]["color"]
    except Exception:
        pass
    return "#6B7280"
