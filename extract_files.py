from pathlib import Path
from os.path import getmtime, join, splitext, exists
from tarfile import open as tar_open
from collections import defaultdict
from datetime import datetime
from functools import partial
from sys import argv
from json import load as json_load
from mytqdm import tqdm


def extract_files(src, dst, judge_func, *, gz=False, group=False):
    src = src.replace("/", "\\").rstrip("\\") + "\\"
    if group:
        groups = defaultdict(set)
        with tqdm(Path(src).rglob("*"), "搜索", unit="文件", ascii=True) as bar:
            all_files = {str(fn) for fn in bar}
        with tqdm(all_files, "分类", unit="文件", ascii=True) as bar:
            for fn in filter(judge_func, bar):
                ft = datetime.fromtimestamp(getmtime(fn))
                groups[(ft.year, ft.month, ft.day)].add(fn.replace(src, ""))
        with tar_open(dst, "w" + (":gz" if gz else "")) as tarf, tqdm(
            groups.items(), "导出组", ascii=True
        ) as bar:
            for (year, month, day), files in bar:
                tarfn_head = join(
                    f"{str(year).zfill(4)}-{str(month).zfill(2)}", str(day).zfill(2),
                )
                with tqdm(files, "导出文件", leave=False, ascii=True) as inner_bar:
                    for fn in inner_bar:
                        tarf.add(join(src, fn), join(tarfn_head, fn), recursive=False)
    else:
        with tqdm(Path(src).rglob("*"), "搜索", unit="文件", ascii=True) as bar:
            all_files = {str(fn) for fn in bar}
        with tqdm(all_files, "分类", unit="文件", ascii=True) as bar:
            valid_files = {fn.replace(src, "") for fn in filter(judge_func, bar)}
        with tar_open(dst, "w" + (":gz" if gz else "")) as tarf, tqdm(
            valid_files, "导出", ascii=True
        ) as bar:
            for fn in bar:
                tarf.add(join(src, fn), fn, recursive=False)


def main(paras: dict):
    # ====时间范围总控制====
    start, end = None, None
    if exists("time_range.json"):
        with open("time_range.json", "r", encoding="utf-8") as tf:
            time_range = json_load(tf)
        if time_range["time_range"] and all(time_range["time_range"]):
            start, end = (
                datetime.strptime(i, "%Y-%m-%d %H:%M:%S") for i in paras["time_range"]
            )
    # ====选择时间范围====
    if paras["time_range"] and all(paras["time_range"]):
        start, end = (
            datetime.strptime(i, "%Y-%m-%d %H:%M:%S") for i in paras["time_range"]
        )
    elif not (start and end):
        start, end = (datetime.min, datetime.max)
    # ====开始提取====
    extract_files(
        paras["src"],
        paras["dst"],
        partial(filter_by_time, start=start, end=end, exts=paras["exts"]),
        gz=paras["gz"],
        group=paras["group"],
    )


def filter_by_time(file, start, end, exts):
    ft = datetime.fromtimestamp(getmtime(file))
    valid_time = start <= ft and ft <= end
    file_ext = splitext(file)[1].lower()
    valid_ext = (not exts) or (not file_ext) or file_ext in exts
    return valid_time and valid_ext


paras_file = argv[1]
with open(paras_file, "r", encoding="utf-8") as jf:
    paras = json_load(jf)
paras["exts"] = {v.lower() for v in paras["exts"]}
main(paras)
