from pathlib import WindowsPath
from os.path import getmtime, join, splitext, exists, basename
from tarfile import open as tar_open
from collections import defaultdict
from datetime import datetime
from functools import partial
import re
from sys import argv
from json import load as json_load
from importlib import import_module

try:
    import tqdm
except ModuleNotFoundError as e:
    tqdm = import_module("mytqdm")
try:
    from tqdm import tqdm
except ModuleNotFoundError as e:
    from mytqdm import tqdm


def extract_files(src, dst, judge_func, *, gz=False, group=False):
    src = src.replace("/", "\\").rstrip("\\") + "\\"
    if group:
        groups = defaultdict(set)
        with tqdm(WindowsPath(src).rglob("*"), "搜索", unit="文件", ascii=True) as bar:
            all_files = {str(fn) for fn in bar}
        with tqdm(all_files, "分类", unit="文件", ascii=True) as bar:
            for fn in filter(judge_func, bar):
                ft = datetime.fromtimestamp(getmtime(fn))
                groups[(ft.year, ft.month, ft.day)].add(fn.replace(src, ""))
        groups = sorted((k, sorted(v)) for k, v in groups.items())
        with tar_open(dst, "w" + (":gz" if gz else "")) as tarf, tqdm(
            groups, "导出组", ascii=True
        ) as bar:
            for (year, month, day), files in bar:
                tarfn_head = join(
                    f"{str(year).zfill(4)}-{str(month).zfill(2)}", str(day).zfill(2),
                )
                with tqdm(files, "导出文件", leave=False, ascii=True) as inner_bar:
                    for fn in inner_bar:
                        tarf.add(join(src, fn), join(tarfn_head, fn), recursive=False)
    else:
        with tqdm(WindowsPath(src).rglob("*"), "搜索", unit="文件", ascii=True) as bar:
            all_files = {str(fn) for fn in bar}
        with tqdm(all_files, "分类", unit="文件", ascii=True) as bar:
            valid_files = sorted(fn.replace(src, "") for fn in filter(judge_func, bar))
        with tar_open(dst, "w" + (":gz" if gz else "")) as tarf, tqdm(
            valid_files, "导出", ascii=True
        ) as bar:
            for fn in bar:
                tarf.add(join(src, fn), fn, recursive=False)


def common(paras: dict):
    # ====时间范围总控制====
    if exists("time_range.json"):
        with open("time_range.json", "r", encoding="utf-8") as tf:
            time_range = json_load(tf)
        start, end = (
            datetime.strptime(i, "%Y-%m-%d %H:%M:%S") for i in time_range["time_range"]
        )
    else:
        start, end = None, None
    # ====选择时间范围====
    if all(paras["time_range"]):
        start, end = (
            datetime.strptime(i, "%Y-%m-%d %H:%M:%S") for i in paras["time_range"]
        )
    elif not (start and end):
        now = datetime.now()
        start, end = (datetime(now.year, now.month, now.day), now)
    # ====开始提取====
    extract_files(
        paras["src"],
        paras["dst"],
        partial(filter_by_time, start=start, end=end, exts=set(paras["exts"])),
        gz=paras["gz"],
        group=paras["group"],
    )


def negtive_parts(paras: dict):
    name_list = set(paras["valid_names"]) - {""}
    name_list = {re.compile(re.escape(i)) for i in name_list}
    extract_files(
        paras["src"],
        paras["dst"],
        partial(filter_by_name, compiled_name_list=name_list, exts=set(paras["exts"])),
    )


def filter_by_name(file, compiled_name_list, exts):
    valid_ext = (not exts) or splitext(file)[1] in exts
    in_list = next(
        filter(lambda i: i.search(basename(file)), compiled_name_list), False
    )
    return valid_ext and in_list


def filter_by_time(file, start, end, exts):
    ft = datetime.fromtimestamp(getmtime(file))
    valid_time = start <= ft and ft <= end
    valid_ext = (not exts) or splitext(file)[1] in exts
    return valid_time and valid_ext


paras_file = argv[1]
# paras_file = "test.json"
with open(paras_file, "r", encoding="utf-8") as jf:
    paras = json_load(jf)
if "valid_names" in paras.keys():
    negtive_parts(paras)
else:
    common(paras)
