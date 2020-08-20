import traceback

def message(e: Exception):
    if e:
        return ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)[-1])
    else:
        return ""
