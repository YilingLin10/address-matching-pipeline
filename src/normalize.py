from exception import NormalizationError
import re

# USPS Publication 28 - Secondary Unit Designators
USPS_SECONDARY_UNITS = {
    "APARTMENT": "APT",
    "BASEMENT": "BSMT",
    "BUILDING": "BLDG",
    "DEPARTMENT": "DEPT",
    "FLOOR": "FL",
    "FRONT": "FRNT",
    "HANGER": "HNGR",
    "KEY": "KEY",
    "LOBBY": "LBBY",
    "LOT": "LOT",
    "LOWER": "LOWR",
    "OFFICE": "OFC",
    "PENTHOUSE": "PH",
    "PIER": "PIER",
    "REAR": "REAR",
    "ROOM": "RM",
    "SIDE": "SIDE",
    "SLIP": "SLIP",
    "SPACE": "SPC",
    "STOP": "STOP",
    "SUITE": "STE",
    "TRAILER": "TRLR",
    "UNIT": "UNIT",
    "UPPER": "UPPR"
}

# Street type abbreviations
STREET_TYPE_ABBR = {
    "AVENUE": "AVE", "STREET": "ST", "ROAD": "RD", "BOULEVARD": "BLVD",
    "DRIVE": "DR", "COURT": "CT", "PLACE": "PL", "SQUARE": "SQ",
    "LANE": "LN", "TRAIL": "TRL", "PARKWAY": "PKWY", "CIRCLE": "CIR"
}

# Directional abbreviations
DIRECTIONAL_ABBR = {
    "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W",
    "NORTHEAST": "NE", "NORTHWEST": "NW", "SOUTHEAST": "SE", "SOUTHWEST": "SW"
}

def clean(text):
    return re.sub(r"\s+", " ", text.strip().upper()) if text else None

def normalize_component(val, mapping):
    val = clean(val)
    if val is None:
        return None
    if val not in mapping:
        raise NormalizationError(f"Unknown value '{val}' for normalization. '{val}'")
    return mapping[val]

def normalize_tagged_address(tagged):
    return {
        "street_number": clean(tagged.get("AddressNumber")),
        "street_name": clean(tagged.get("StreetName")),
        "street_type": normalize_component(tagged.get("StreetNamePostType"), STREET_TYPE_ABBR),
        "street_predir": normalize_component(tagged.get("StreetNamePreDirectional"), DIRECTIONAL_ABBR),
        "street_postdir": normalize_component(tagged.get("StreetNamePostDirectional"), DIRECTIONAL_ABBR),
        "unit_type": normalize_component(
            tagged.get("OccupancyType"),
            USPS_SECONDARY_UNITS
        ),
        "unit_identifier": clean(tagged.get("OccupancyIdentifier")),
        "city": clean(tagged.get("PlaceName")),
        "state": clean(tagged.get("StateName")),
        "zip": tagged.get("ZipCode")
    }
