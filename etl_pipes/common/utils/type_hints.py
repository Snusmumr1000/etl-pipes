def is_tuple_type_hint(type_hint: type) -> bool:
    return getattr(type_hint, "__origin__", None) is tuple
