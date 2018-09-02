def _parse_subject(uid):
    if isinstance(uid, int):
        return f"<{hex(uid)}>", uid
    else:
        return f"_:{uid}", uid


def _rdf_value(value):
    if isinstance(value, str):
        value = f'"{value}"'
    elif isinstance(value, bool):
        value = f'"{str(value).lower()}"'
    elif isinstance(value, int):
        value = f'"{int(value)}"^^<xs:int>'
    elif isinstance(value, float):
        value = f'"{value}"^^<xs:float>'
    return value


def _raw_value(value):
    if isinstance(value, str):
        value = f'"{value}"'
    elif isinstance(value, bool):
        value = f'{str(value).lower()}'
    elif isinstance(value, int):
        value = f'{int(value)}'
    elif isinstance(value, float):
        value = f'{value}'
    return value
