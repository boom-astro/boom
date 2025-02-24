import collections.abc as collections
import yaml
from pathlib import Path
import os

# Cache loading of environment
_cache = {}


def recursive_update(d, u):
    # Based on https://stackoverflow.com/a/3233356/214686
    for k, v in u.items():
        if isinstance(v, collections.Mapping):
            r = recursive_update(d.get(k, {}) or {}, v)
            d[k] = r
        else:
            d[k] = u[k]
    return d


def relative_to(path, root):
    p = Path(path)
    try:
        return p.relative_to(root)
    except ValueError:
        return p


class Config(dict):
    """To simplify access, the configuration allows fetching nested
    keys separated by a period `.`, e.g.:

    >>> cfg['app.db']

    is equivalent to

    >>> cfg['app']['db']

    """

    def __init__(self, config_files=None):
        dict.__init__(self)
        if config_files is not None:
            cwd = os.getcwd()
            config_names = [relative_to(c, cwd) for c in config_files]
            print(f"  Config files: {config_names[0]}")
            for f in config_names[1:]:
                print(f"                {f}")
            self["config_files"] = config_files
            for f in config_files:
                self.update_from(f)

    def update_from(self, filename):
        """Update configuration from YAML file"""
        if os.path.isfile(filename):
            more_cfg = yaml.full_load(open(filename))
            recursive_update(self, more_cfg)

    def __getitem__(self, key):
        keys = key.split(".")

        val = self
        for key in keys:
            if isinstance(val, dict):
                val = dict.__getitem__(val, key)
            else:
                raise KeyError(key)

        return val

    def get(self, key, default=None, /):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def show(self):
        """Print configuration"""
        print()
        print("=" * 78)
        print("Configuration")
        for key in self:
            print("-" * 78)
            print(key)

            if isinstance(self[key], dict):
                for key, val in self[key].items():
                    print("  ", key.ljust(30), val)

        print("=" * 78)


def load_config(config_files=["config.yaml"]):
    """
    Load config and secrets
    """
    if not _cache:
        missing = [cfg for cfg in config_files if not os.path.isfile(cfg)]
        if missing:
            print(f'Missing config files: {", ".join(missing)}; continuing.')
        if "config.yaml" in missing:
            print(
                "Warning: You are running on the default configuration. To configure your system, "
                "please copy `config.defaults.yaml` to `config.yaml` and modify it as you see fit."
            )

        all_configs = [
            Path("config.defaults.yaml"),
        ] + config_files
        all_configs = [cfg for cfg in all_configs if os.path.isfile(cfg)]
        all_configs = [os.path.abspath(Path(c).absolute()) for c in all_configs]

        cfg = Config(all_configs)
        _cache.update({"cfg": cfg})

    return _cache["cfg"]