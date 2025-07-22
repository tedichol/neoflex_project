def db_str_config(file_path) -> str:
    db_params = dict()
    with open(file_path, "r", encoding="utf-8") as conn_params:
        i=0
        for line in conn_params:
            if i > 6 : break
            line = line.strip()
            k, v = line.split("=")
            db_params[k] = v
            i += 1
    return (f"{db_params['db_type']}+{db_params['driver']}://"
        f"{db_params['user']}:{db_params['password']}@{db_params['host']}:"
            f"{db_params['port']}/{db_params['db_name']}")