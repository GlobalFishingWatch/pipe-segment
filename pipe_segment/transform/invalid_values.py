from decimal import Decimal


def validate_field(is_invalid):
    """
    Returns a function which checks a value using the `is_invalid` function. If
    the `is_invalid` function returns true for a given value, it is converted
    to `None` instead. If not, the original value is returned.

    Args:
        is_invalid (Callable): Function which returns True when a given value is invalid,
            False otherwise.

    Returns:
        Callable: A validator function that returns None for invalid values, or the original value.

    Examples:
        >>> validator = validate_field(lambda x: x > 5)
        >>> validator(1)
        1
        >>> validator(6)
        None
    """
    return lambda value: None if is_invalid(value) else value


def float_to_fixed_point(value, precision):
    """
    Converts a given floating point value to a fixed precision decimal.

    Args:
        value (float or Decimal): The value to convert.
        precision (int): The number of decimal places to keep.

    Returns:
        Decimal: The value rounded to the specified precision.

    Examples:
        >>> float_to_fixed_point(120.034, 1)
        Decimal('120.0')
    """
    precision_format_string = "{{0:.{}f}}".format(precision)
    precision_decimal = Decimal(precision_format_string.format(1))
    return Decimal(value).quantize(precision_decimal)


def validate_fixed_position_field(precision, is_invalid):
    """
    Specializes `validate_field` by converting the value to a fixed point decimal
    before calling the `is_invalid` function.

    Args:
        precision (int): Number of decimal places to use for fixed point conversion.
        is_invalid (Callable): Function that returns True if the value is invalid.

    Returns:
        Callable: A validator function for fixed point fields.

    Examples:
        >>> validator = validate_fixed_position_field(0, lambda x: x > 5)
        >>> validator(5.1)
        5.1
        >>> validator(6)
        None
    """
    return validate_field(
        lambda value: is_invalid(float_to_fixed_point(value, precision))
    )


def validate_all_fields_record(is_invalid):
    """
    Returns a function that checks if all specified fields in a record are invalid
    according to the provided `is_invalid` function. If all are invalid, those fields
    are set to `None` in the returned record; otherwise, the original values are kept.

    Args:
        is_invalid (Callable): Function that returns True when a given value is invalid.

    Returns:
        Callable: A function that takes a list of fields and a record (dict), and returns
        a new record with the specified fields set to None if all are invalid.

    Examples:
        >>> validator = validate_all_fields_record(lambda x: x == 0)
        >>> validator(["lat", "lon"], {"lat": 0, "lon": 0})
        {'lat': None, 'lon': None}
        >>> validator(["lat", "lon"], {"lat": 1, "lon": 0})
        {'lat': 1, 'lon': 0}
    """
    def validate_multiple_fields(fields, record):
        not_valid = all(is_invalid(record[field]) for field in fields)
        return {field: None if not_valid else record[field] for field in fields}
    return validate_multiple_fields


INVALID_VALUE_RULES_BY_MESSAGE_TYPE = {
    # Class A Position Report
    "AIS.1": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(1, lambda x: x < 0 or x >= 360),
        "heading": validate_fixed_position_field(0, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(1, lambda x: x < 0 or x >= 102.3),
    },
    "AIS.2": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(1, lambda x: x < 0 or x >= 360),
        "heading": validate_fixed_position_field(0, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(1, lambda x: x < 0 or x >= 102.3),
    },
    "AIS.3": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(1, lambda x: x < 0 or x >= 360),
        "heading": validate_fixed_position_field(0, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(1, lambda x: x < 0 or x >= 102.3),
    },
    # Base Station Reports
    "AIS.4": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
    },
    "AIS.11": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
    },
    # Class A Ship Static and Voyage Data
    "AIS.5": {
        "imo": validate_field(
            lambda x: not ("0000000001" <= x.zfill(10) < "1073741824")
        ),
        "callsign": validate_field(lambda x: x == "@@@@@@@"),
        "shipname": validate_field(lambda x: x == "@@@@@@@@@@@@@@@@@@@@"),
        "destination": validate_field(lambda x: x == "@@@@@@@@@@@@@@@@@@@@"),
    },
    # Search and Rescue Aircraft Position Report
    "AIS.9": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(1, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(1, lambda x: x < 0 or x >= 102.3),
    },
    # GNSS Broadcast
    "AIS.17": {
        "lon": validate_fixed_position_field(2, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(2, lambda x: x <= -91 or x >= 91),
    },
    # Classs B Position Report
    "AIS.18": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(1, lambda x: x < 0 or x >= 360),
        "heading": validate_fixed_position_field(0, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(1, lambda x: x < 0 or x >= 102.3),
    },
    # Class B Exteded Position Report
    "AIS.19": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(1, lambda x: x < 0 or x >= 360),
        "heading": validate_fixed_position_field(0, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(1, lambda x: x < 0 or x >= 102.3),
        "shipname": validate_field(lambda x: x == "@@@@@@@@@@@@@@@@@@@@"),
    },
    # ATON Report
    "AIS.21": {
        "lon": validate_fixed_position_field(5, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(5, lambda x: x <= -91 or x >= 91),
        "shipname": validate_field(lambda x: x == "@@@@@@@@@@@@@@@@@@@@"),
    },
    # Class B Ship Static and Voyage Data
    "AIS.24": {
        "callsign": validate_field(lambda x: x == "@@@@@@@"),
        "shipname": validate_field(lambda x: x == "@@@@@@@@@@@@@@@@@@@@"),
    },
    # Long Range AIS Broadcast
    "AIS.27": {
        "lon": validate_fixed_position_field(2, lambda x: x <= -181 or x >= 181),
        "lat": validate_fixed_position_field(2, lambda x: x <= -91 or x >= 91),
        "course": validate_fixed_position_field(0, lambda x: x < 0 or x >= 360),
        "speed": validate_fixed_position_field(0, lambda x: x < 0 or x >= 63),
    },
    # General VMS
    "VMS": {
        "lon": validate_field(lambda x: x < -180 or x > 180),
        "lat": validate_field(lambda x: x < -90 or x > 90),
        "course": validate_field(lambda x: x < 0 or x >= 360),
        "speed": validate_field(lambda x: x < 0),
    },
}

INVALID_GROUP_RULES_BY_MESSAGE_TYPE = {
    "VMS": {
        # VMS messages with both lat and lon set to 0 are invalid
        # and should be set to None
        ("lat", "lon"): validate_all_fields_record(lambda x: x == 0),
    }
}


def apply_field_validators(element):
    """
    Applies the field validators to the given element. The field validators
    are defined in the `INVALID_VALUE_RULES_BY_MESSAGE_TYPE` dictionary.

    Args:
        element (dict): The element to apply the field validators to.

    Returns:
        dict: The element with the field validators applied.
    """
    field_validators = INVALID_VALUE_RULES_BY_MESSAGE_TYPE.get(element["type"])

    if field_validators is None:
        return element

    for field, validator in field_validators.items():
        unfiltered_value = element.get(field)
        if unfiltered_value is not None:
            element[field] = validator(unfiltered_value)

    return element


def apply_group_validators(element):
    """
    Applies the group validators to the given element. The group validators
    are defined in the `INVALID_GROUP_RULES_BY_MESSAGE_TYPE` dictionary.

    Args:
        element (dict): The element to apply the group validators to.

    Returns:
        dict: The element with the group validators applied.
    """
    group_validators = INVALID_GROUP_RULES_BY_MESSAGE_TYPE.get(element["type"])

    if group_validators is None:
        return element

    # Making a copy of the element to avoid modifying the original
    element = element.copy()
    for fields, validator in group_validators.items():
        # Check if any of the fields are None
        # If any field is None, we don't need to apply the validator
        if any(element.get(f) is None for f in fields):
            continue

        # Apply the validator to the element
        record = validator(fields, element)
        # Update the element with the validated record
        for field in fields:
            element[field] = record.get(field)

    return element


def filter_invalid_values(element):
    """
    Applies the field and record validators to the given element. The field
    validators are defined in the `INVALID_VALUE_RULES_BY_MESSAGE_TYPE` dictionary
    and the record validators are defined in the `INVALID_GROUP_RULES_BY_MESSAGE_TYPE`
    dictionary.

    Args:
        element (dict): The element to apply the field and record validators to.

    Returns:
        dict: The element with the field and record validators applied.
    """
    element = apply_field_validators(element)
    element = apply_group_validators(element)
    return element
