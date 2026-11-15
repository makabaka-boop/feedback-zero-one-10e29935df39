"""Validation of the cluster's initial version description.

The init document maps a source name to its non-empty version array:

    {"A": [{"rows": [], "needs": {}},
           {"rows": ["a1"], "needs": {"B": 1}}],
     "B": [{"rows": [], "needs": {}},
           {"rows": ["b1"], "needs": {}}]}

The array index *is* the version number.  Version 0 is fixed to
``rows=[]``/``needs={}``.  A ``needs`` entry names another source and the
lowest version of it that must be selected alongside; an omitted entry means
version 0.  Requirements must reference existing versions, must not exceed
the requiring version itself, and must be non-decreasing with the version.
"""

from .common import InputError, is_name, is_version, require


def _source_path(name):
    return "$." + name if is_name(name) else "$"


def validate_init(doc):
    """Raise InputError with a zero-based JSONPath on the first problem."""
    require(isinstance(doc, dict), "$")
    require(len(doc) > 0, "$")

    for name, versions in doc.items():
        base = _source_path(name)
        if not is_name(name):
            raise InputError("$")
        require(isinstance(versions, list) and len(versions) > 0, base)
        prev_needs = {}
        for index, ver in enumerate(versions):
            vpath = "%s[%d]" % (base, index)
            require(isinstance(ver, dict), vpath)
            require(set(ver.keys()) == {"rows", "needs"}, vpath)
            rows, needs = ver["rows"], ver["needs"]
            require(
                isinstance(rows, list)
                and all(isinstance(row, str) for row in rows),
                vpath + ".rows",
            )
            require(isinstance(needs, dict), vpath + ".needs")
            if index == 0:
                # Version 0 is the empty base version.
                require(rows == [] and needs == {}, vpath)
            for dep, minimum in needs.items():
                dpath = "%s.needs.%s" % (vpath, dep) if is_name(dep) else vpath + ".needs"
                require(is_name(dep) and dep in doc, dpath)
                require(is_version(minimum) and minimum >= 0, dpath)
                # A source's requirement on itself may not exceed the version
                # being selected (needs <= own index for self references).
                if dep == name:
                    require(minimum <= index, dpath)
                # The referenced version must exist.
                require(minimum < len(doc[dep]), dpath)
            # Requirements are non-decreasing with the version: a dependency
            # that is dropped counts as returning to 0.
            for dep in set(prev_needs) | set(needs):
                dpath = "%s.needs.%s" % (vpath, dep) if is_name(dep) else vpath + ".needs"
                require(needs.get(dep, 0) >= prev_needs.get(dep, 0), dpath)
            prev_needs = needs
