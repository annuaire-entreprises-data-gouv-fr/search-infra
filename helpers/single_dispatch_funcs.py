def unique_list(lst):
    ulist = []
    [ulist.append(x) for x in lst if x not in ulist]
    return ulist


def unique_string(a):
    return " ".join(unique_list(a.strip().split(","))).strip()


def get_string(string):
    if string is None:
        return ""
    return string


def dict_from_row(row):
    return dict(zip(row.keys(), row))
