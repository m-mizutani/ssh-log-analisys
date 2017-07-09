# ssh-log-analisys



prerequisite
-------

- Python3
- pip
- GeoIP

GeoIP setup in macOS with Homebrew

```
$ brew install geoip geoipupdate
$ geoipupdate
```

usage
--------

setup

```
$ pip install -r requirements.txt
```

Parse log from `<log_dir>` and output parsed logs to `<output file>` in MessagePack format.

```
$ ./parse.py <log dir> <output file>
```

