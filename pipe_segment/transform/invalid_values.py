import decimal


def validate_field(is_invalid):
    """
    Returns a function which checks a value using the `is_invalid` function. If
    the `is_invalid` function returns true for a given value, it is converted
    to `None` instead. If not, the original value is returned.

    Arguments:
    `is_invalid`: Function which returns true when a given value is invalid,
    false otherwise

    Examples:
    ```
    validator = validate_field(lambda x: x > 5)
    validator(1) -> 1
    validator(6) -> None
    ```
    """
    return lambda value: None if is_invalid(value) else value


def float_to_fixed_point(value, precision):
    """
    Converts a given floating point value to a fixed precision decimal.

    Examples:
    ```
    float_to_fixed_point(120.034, 1) -> Decimal(120.0)
    ```
    """
    # We need to generate a string which contains a number with a given number
    # of values after the decimal point, so that we can quantize the input
    # value to that same amount of decimal places. For example, for precision 2
    # we get `{0:.2f}`, which when applied as a format to a number `1` we get
    # `"1.00"`
    precision_format_string = "{{0:.{}f}}".format(precision)
    precision_decimal = decimal.Decimal(precision_format_string.format(1))
    return decimal.Decimal(value).quantize(precision_decimal)


def validate_fixed_position_field(precision, is_invalid):
    """
    This is a specialization for `validate_field`, which converts the value to
    a fixed point decimal before calling the `is_invalid` function.

    Examples:
    ```
    validator = validate_fixed_position_field(0, lambda x: x > 5)
    validator(5.1) -> 5.1
    validator(6) -> None
    ```
    """
    return validate_field(
        lambda value: is_invalid(float_to_fixed_point(value, precision))
    )


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
}


def filter_invalid_values(element):
    field_validators = INVALID_VALUE_RULES_BY_MESSAGE_TYPE.get(element["type"])

    if not field_validators:
        return element

    for field, validator in field_validators.items():
        unfiltered_value = element.get(field)
        if unfiltered_value is not None:
            element[field] = validator(unfiltered_value)

    return element
