"""
Module for parsing raw NHTSA API data into a structured, analytical format.
"""

from typing import Any, Dict, List, Optional

# Field mapping from raw API keys to human-readable names.
ANALYTICAL_MAPPING: Dict[str, str] = {
    # Vehicle Specs
    "VEHTWT": "spec_weight_kg",
    "VEHLEN": "spec_length_mm",
    "VEHWID": "spec_width_mm",
    "WHLBAS": "spec_wheelbase_mm",
    "ENGINED": "spec_engine_desc",
    "ENGDSP": "spec_engine_disp",
    "BODYD": "spec_body_type",
    "VIN": "spec_vin",
    # Crash Physics
    "VEHSPD": "crash_speed_kph",
    "CRBANG": "crash_angle_deg",
    "PDOF": "crash_pdof_deg",
    # Damage Data
    "VDI": "damage_vdi_code",
    "TOTCRV": "damage_total_crush",
    # Crush Profile
    "DPD1": "crush_profile_c1",
    "DPD2": "crush_profile_c2",
    "DPD3": "crush_profile_c3",
    "DPD4": "crush_profile_c4",
    "DPD5": "crush_profile_c5",
    "DPD6": "crush_profile_c6",
    # Vehicle Dimensions (Before/After Crash)
    "BX1": "dim_bx1", "AX1": "dim_ax1",
    "BX2": "dim_bx2", "AX2": "dim_ax2",
    "BX3": "dim_bx3", "AX3": "dim_ax3",
}


def get_value_case_insensitive(
    record: Dict[str, Any], possible_keys: List[str]
) -> Any:
    """Finds and returns a value from a dictionary using a list of possible keys."""
    for key in possible_keys:
        if key in record and record[key] is not None:
            return record[key]
    return None


def _extract_analytical_data(raw_veh: Dict[str, Any]) -> Dict[str, Any]:
    """Extracts and transforms data from a raw vehicle record for analysis."""
    extracted: Dict[str, Any] = {}
    for api_key, readable_key in ANALYTICAL_MAPPING.items():
        val = raw_veh.get(api_key)
        if val is not None and val != "":
            # Convert numeric fields to float
            if any(x in readable_key for x in ["_kg", "_mm", "_kph", "_deg", "_c", "dim_"]):
                try:
                    extracted[readable_key] = float(val)
                except (ValueError, TypeError):
                    extracted[readable_key] = val
            else:
                extracted[readable_key] = val
        else:
            extracted[readable_key] = None
    return extracted


def parse_record(
    test_id: int, api_data: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """Parses a single raw API response into a structured analytical record.

    Args:
        test_id: The test number for the record.
        api_data: The raw JSON dictionary from the 'metadata/{id}' endpoint.

    Returns:
        A dictionary containing the cleaned and structured data, or None if
        the record is invalid.
    """
    results_wrapper = api_data.get("results", [])
    if not results_wrapper:
        print(f"    - DEBUG_PARSE: test_id {test_id} has empty 'results' wrapper.")
        return None

    first_wrapper = results_wrapper[0]
    test_info = first_wrapper.get("TEST", {})
    link_info = first_wrapper.get("URL", {})
    report_list = first_wrapper.get("REPORTS", [])
    vehicle_list = first_wrapper.get("VEHICLE", [])
    if not vehicle_list:
        print(f"    - DEBUG_PARSE: test_id {test_id} has empty 'VEHICLE' list.")
        return None

    target_vehicle = None
    for veh in vehicle_list:
        make = get_value_case_insensitive(veh, ["MAKED", "Make"])
        year = get_value_case_insensitive(veh, ["YEAR", "Year"])
        make_str = str(make).upper() if make else ""
        try:
            year_int = int(year) if year else 0
        except (ValueError, TypeError):
            year_int = 0
        if make_str != "NHTSA" and year_int > 0:
            target_vehicle = veh
            break

    if not target_vehicle:
        # Fallback to the first vehicle in the list if no specific target is found
        if vehicle_list:
            target_vehicle = vehicle_list[0]
        else:
            print(f"    - DEBUG_PARSE: test_id {test_id} no target vehicle and empty vehicle_list for fallback.")
            return None

    if target_vehicle:
        analytical_data = _extract_analytical_data(target_vehicle)
        return {
            # Identifiers
            "test_no": test_id,
            # Test Info
            "test_title": test_info.get("TITLE"),
            "test_type": test_info.get("TSTTYPD"),
            "test_config": test_info.get("TSTCFND"),
            # Vehicle Info
            "make": get_value_case_insensitive(target_vehicle, ["MAKED", "Make"]) or "Unknown",
            "model": get_value_case_insensitive(target_vehicle, ["MODELD", "Model"]) or "Unknown",
            "model_year": get_value_case_insensitive(target_vehicle, ["YEAR", "Year"]),
            # Downloadable Content
            "links": link_info,
            "reports": report_list,
            # Analytical Data
            **analytical_data,
            # Raw Backup
            "raw_data": target_vehicle,
        }
    print(f"    - DEBUG_PARSE: test_id {test_id} reached end of parse_record without returning a record.")
    return None