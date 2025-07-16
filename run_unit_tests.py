#!/usr/bin/env python3 
#
# This script runs unit tests for a web application by reading a list of test cases,
# making HTTP GET requests to a server, comparing the responses against reference outputs,
# and saving the results, differences, and timing information in a specified output directory.
# It supports filtering test cases by unit name and generates summary and detailed reports.
#
# PROD (C# version) [didn't work on PROD: why?]
# ./run_unit_tests.py --server https://dev.ictv.global --in test_cases.csharp.txt
#
# LOGAN1 (php version)
# ./run_unit_tests.py --server https://logan1.ictv.global --in test_cases.php.txt
#
# Generating test_ref:
# ./run_unit_tests.py --server https://data.ictv.global --in test_cases.csharp.txt --out_dir test_ref  --no_compare
#
# TODO
#   IN_PROCESS mode to re-apply decode w/o re-fetching from server
#   test case
# ./run_unit_tests.py --in test_cases.csharp.txt --server https://dev.ictv.global/ICTV --out_dir test_out.remove_csharp --verbose --only St.-Louis-encephalitis-virus --scrub
# dwdiff -P --color test_ref/taxonomy.taxonomyHistoryRegression_40_1-40_1_St.-Louis-encephalitis-virus.decode.txt test_out.remove_csharp/taxonomy.taxonomyHistoryRegression_40_1-40_1_St.-Louis-encephalitis-virus.decode.txt
#
import argparse
import os
import requests
import subprocess
import time
import json
import re
from collections import defaultdict

# linux cmdline colors
GREEN = "\033[92m"
RED = "\033[91m"
BLUE = "\033[34m"
ORANGE = "\033[38;5;208m" # 8-bit color
#ORANGE = "\033[38;2;255;165;0m" # 24-bit true color
RESET = "\033[0m"

def parse_args():
    parser = argparse.ArgumentParser(description="Unit test runner for web application")
    parser.add_argument("--out_dir", "-o", default="./test_dev_csharp", help="Directory to save test outputs")
    parser.add_argument("--ref_dir", "-r", default="./test_ref", help="Directory containing reference outputs")
    parser.add_argument("--in_file", "-i", dest="in_file", default="test_cases.csharp.txt", help="Input file with test cases")
    parser.add_argument("--server", "-s", default="https://dev.ictv.global", help="Base server URL")
    parser.add_argument("--only", "-u", nargs="*", help="Optional list of specific unit names to run")
    parser.add_argument("--update", action="store_true", help="Only run test cases with NO output file")
    parser.add_argument("--scrub", action="store_true", help="Turns on aggressive content scrubbing, to reduce flase negatives")
    parser.add_argument("--exclude", "-e", nargs="*", help="Optional list of specific unit names to NOT run")
    parser.add_argument("--verbose", "-v", action="store_true", help="Print results as test cases are executed")
    parser.add_argument("--show_diff", "-d", action="store_true", help="Print path to diff when fails")
    parser.add_argument("--no_compare", action="store_true", help="Do not compare against reference timings (useful when regenerating test_ref)")
    parser.add_argument("--no_pretty", action="store_true", help="Do not require successful parse and pretty print of json")
    parser.add_argument("--pct_change", default="15", help="How much slower/faster a query must run to be flagged for time change")
    parser.add_argument("--ref_report_name", default="report.txt")
    parser.add_argument("--summary_name",    default="report.summary.txt")
    parser.add_argument("--report_name",     default="report.txt")
    return parser.parse_args()


# ----------------------------------------------------------------------
# Helper function for get-taxon-history
# ----------------------------------------------------------------------
def parse_custom_case_line(line: str):
    """
    Handle the 7 column txt file specific to get-taxon-history test cases
    """
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 7:
        raise ValueError(f"Expected 7 columns, got {len(parts)}: {line!r}")

    bin_no, tax_id, bin_name, key_taxon, unit, case, url = parts
    return {
        "bin":        bin_name.strip(),
        "key_taxon":  key_taxon.strip(),
        "unit":       unit.strip(),
        "case":       case.strip(),
        "url":        url.strip()
    }


# ----------------------------------------------------------------------
# Helper function for get-taxon-history
# ----------------------------------------------------------------------
def build_out_dirs(root: str, meta: dict) -> str:
    """
    Build the directory where get-taxon-history test cases will be stored

    Example: test_ref/Papovarviridae/ICTV19710002=family=Papovaviridae/test_files
    """
    path = os.path.join(root, meta["bin"], meta["key_taxon"])
    os.makedirs(path, exist_ok=True)
    return path


def read_test_cases(filename):
    test_cases = []
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Condition to handle get-taxon-history 7 column txt file
            if line.count("\t") == 6:
                test_cases.append(parse_custom_case_line(line))

            # Original condition that handles the legacy 3 columns
            else:
                unit, case, url = line.split('\t')
                test_cases.append({'unit': unit, 'case': case, 'url': url})
    return test_cases


def fetch_url(server, url):
    full_url = server.rstrip('/') + '/' + url.lstrip('/')
    try:
        response = requests.get(full_url)
        if response.text.startswith("HTTP"):
            printf(f"{RED}ERROR_CONNECTION{RESET}: {response}")
        return response.text, response.status_code
    except Exception as e:
        return str(e), 500

def pretty_json_or_raw(content,filename,verbose,no_pretty):
    if verbose: print(f"# -- pretty-ing {filename}")
    try:
        truncated = content#[:-1]
        parsed = json.loads(truncated)
        return json.dumps(parsed, indent=4)
    except json.JSONDecodeError as ex:
        if verbose:
            print(f"#-- JSON parse failed: {filename}")
            print(f"#-- JSON parse ERROR: {ex}")
        filename = filename+".bad"
        with open(filename,'w') as f:
            f.write(content)
        if verbose or not no_pretty:
            print(f"#-- wrote {len(content)} bytes to {filename}")
        if not no_pretty:
            raise
        return content
    
def save_file(path, content, verbose=False):
    if verbose:
        print(f"#-- save: {path}")
    with open(path, 'w') as f:
        f.write(content)
    if verbose:
        print(f"#-- wrote {len(content)}")

def load_file(path, verbose=False):
    if verbose:
        print(f"#-- load: {path}")
    with open(path, 'r') as f:
        content=f.read()
    if verbose:
        print(f"#-- loaded {len(content)}")
    return content


def run_diff(ref_file, new_file):
    result = subprocess.run(['diff', '-w', ref_file, new_file], capture_output=True, text=True)
    return result.returncode, result.stdout


def run_dwdiff(ref_file, new_file):
    cmd = f"dwdiff -P --color {ref_file} {new_file}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout

def run_dwdiff_short(ref_file, new_file):
    cmd = f"diff -u {ref_file} {new_file} | dwdiff -u -P --color "
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout

#
# PHP outputs the data with some things [<>"] being Unicode encode
#
# this script decodes those things (plus removing the "\" before the "/" in close tags!)
# so that output from PHP can be compared with diff to output from C#
#
# TODO: Is there a way to generalize this code, possibly with a python library, to strip all escapes without having to hardcode them?
def decode_unicode_escapes(content,scrub):
    """Undo \\u003C, \\u003E, etc., and strip unused JSON keys."""
    try:
        content = json.loads(f'"{content}"')
    except json.JSONDecodeError:
        content = (content
                   .replace('\\u00eb', '\u00eb')
                   .replace('\\u2011', '\u2011')
                   .replace('\\u0027', "'")
                   .replace('\\u0026', '&')
                   .replace('\\u003E', '>')
                   .replace('\\u003C\\', '<')
                   .replace('\\u003C', '<')
                   .replace('\\u0022', '\\"'))
    # remove "jsonID": null,  "jsonLineage": null (with or without trailing comma)
    content = re.sub(r'\s*"json(?:ID|Lineage)"\s*:\s*null\s*,?', '', content)
    # clean up double‑commas or ",}" artifacts
    content = content.replace(',,', ',').replace(',}', '}')
    # un-escape "/" - php added a "\" escape to "/"
    # example: get_release_history.default : "notes":"Plenary session vote 12\/16 September 1975 in Madrid (MSL #03)"
    content = content.replace('\\/', '/')
    
    # example: changed "\"LuBoV, RBoV\"" to "LuBoV, RBoV" on csharp service
    content = re.sub(r'"\\"([^"]*?)\\""', r'"\1"', content)

    # additional (aggressive) changes
    if scrub:
        content = re.sub(r'"isMoved":(false|true)', '"isMoved":"SCRUBBED"',content)
        content = re.sub(r'"isLineageUpdated":(false|true)', '"isLineageUpdated":"SCRUBBED"',content)
        content = re.sub(r'"previousLineage":"[^"]*"', '"previousLineage":"SCRUBBED"',content)
        content = re.sub(r'"mslReleaseNumber": *[0-9]+', '"mslReleaseNumber":"SCRUBBED"',content)
        # these regex's handle embedded, escaped quotes: "Renamed \"T4-like viruses\" to T4likevirus"
        content = re.sub(r'"prevNotes":(null|"((?:[^"\\]|\\.)*)")', '"prevNotes":"SCRUBBED"',content)
        content = re.sub(r'"nextNotes":(null|"((?:[^"\\]|\\.)*)")', '"nextNotes":"SCRUBBED"',content)
    return content

# def decode_unicode_escapes(content):
#     try:
#         return json.loads(f'"{content}"')
#     except json.JSONDecodeError:
#         return content.replace('\\u003E', '>').replace('\\u003C\\', '<').replace('\\u003C', '<').replace('\\u0022', '\\"')
    
def load_ref_timings(ref_report_path,verbose=False):
    """
    Return a dict {(unit, case): seconds} from ref-dir/report.txt.
    """
    timings = {}
    if not os.path.isfile(ref_report_path):
        return timings

    with open(ref_report_path, "r") as f:
        linenum = 0
        for line in f:
            linenum = linenum+1
            line = line.strip()
            if not line or line.startswith(("SERVER:", "IN_FILE:")):
                if verbose: print(f"{ref_report_path}:{linenum} skip: {line}")
                continue

            parts = line.split('\t')
            # need at least 4 columns: RESULT, ELAPSED, [CHANGE,]UNIT, CASE
            if len(parts) < 4:
                if verbose: print(f"{ref_report_path}:{linenum} skip(len not 4,5): {line}")
                continue
            else:
                # unpack version w/ time change column
                # RESULT = parts[0], ELAPSED = parts[1], UNIT = parts[3], CASE = parts[4]
                status, elapsed_str, change_str, unit, case = parts[:5]
            try:
                elapsed = float(elapsed_str.rstrip('s'))
            except ValueError:
                # skip lines where we can't parse the time
                if verbose: print(f"{ref_report_path}:{linenum} skip (can't parse time): {line}")
                continue

            if verbose: print(f"{ref_report_path}:{linenum} ({unit},{case}={elapsed}")
            timings[(unit, case)] = {"elapsed": elapsed, "status": status}

    return timings

# If there is a network failure, a file can end up with "HTTP.." instead of valid JSON. 
def file_starts_with_http(path):
    try:
        with open(path, 'rb') as f:
            return f.read(4) == b'HTTP'
    except Exception:
        return False
    
# ----------------------------------------------------------------------
#  Normalize “get-taxon-history” JSON so PHP & C# diff cleanly
# ----------------------------------------------------------------------

def _normalize_taxon_history(obj, path=()):
    """
    Convert a *get-taxon-history* response (C# **or** PHP) to a
    canonical shape so a textual diff only shows true data changes.

    Rules implemented:
      • Drop C#-only fields: treeID in releases[], selectedTaxon, rankName & year in taxa[].
      • Drop PHP-only fields: isCurrent, isVisible (releases[]),
                              isSelected, prevLineageRanks, rankName   (taxa[]).
      • Rename PHP field spellings to C# spellings.
    """
    # ----------- dict -----------
    if isinstance(obj, dict):
        out = {}

        for k, v in obj.items():
            # — whole-object drop —
            if path == () and k == "selectedTaxon":
                continue

            # — universal key drops —
            if k in {"isCurrent", "isVisible",        # releases
                     "isSelected", "prevLineageRanks",
                     "prevLineageNames", "previousLineage", "rankName"}:
                continue

            # — context-sensitive drops —
            if path[:1] == ("releases",) and k == "treeID":
                continue
            if path[:1] == ("taxa",) and k == "year":
                continue

            # — renames for PHP → C# —
            ren = {
                "lineageNames"  : "lineage",
                "mslReleaseNum" : "mslReleaseNumber",
                "prevNames"        : "previousNames",
            }
            new_k = ren.get(k, k)
            out[new_k] = _normalize_taxon_history(v, path + (new_k,))

        return out

    # ----------- list -----------
    if isinstance(obj, list):
        return [_normalize_taxon_history(x, path + (path[-1] if path else "",))
                for x in obj]

    # ----------- scalars --------
    return obj
    
# ----------------------------------------------------------------------
#
# MAIN
#
# ----------------------------------------------------------------------

def main():
    args = parse_args()

    do_compare = not args.no_compare
    ref_path     = os.path.join(args.ref_dir, args.ref_report_name)
    ref_timings  = load_ref_timings(ref_path,verbose=False) if do_compare else {}

    if args.verbose:
        print(f"do_compare: {do_compare}")
        print(f"update:     {args.update}")
        print(f"scrub:      {args.scrub}")
    os.makedirs(args.out_dir, exist_ok=True)

    #
    # load test unit/case list adn filter
    #
    test_cases = read_test_cases(args.in_file)
    print(f"# loaded: {len(test_cases)} test cases from {args.in_file}")
    if args.only:
        compiled = [re.compile(pat, re.IGNORECASE) for pat in args.only]
        # [tc for tc in test_cases if any(rx.search(tc[1] for rx in compiled)]
        def matches(tc):
            return any(rx.search(tc["unit"]) or rx.search(tc["case"]) for rx in compiled)
        test_cases = [tc for tc in test_cases if matches(tc)]
    if args.exclude:
        compiled = [re.compile(pat, re.IGNORECASE) for pat in args.exclude]
        # [tc for tc in test_cases if any(rx.search(tc[1] for rx in compiled)]
        def matches(tc):
            return any(not(rx.search(tc["unit"]) or rx.search(tc["case"])) for rx in compiled)
        test_cases = [tc for tc in test_cases if matches(tc)]
    print(f"# filtered: {len(test_cases)} test cases to run")

    #
    # create output data structures
    #
    summary = defaultdict(lambda: {'pass': 0, 'fail': 0, 'http':0})
    detailed_results = []

    report_path = os.path.join(args.out_dir, args.report_name)
    f_report = open(report_path, 'w')
    f_report.write(f"SERVER:{args.server}"+"\n")
    f_report.write(f"IN_FILE:{args.in_file}"+"\n")

    #
    # iterate over test cases
    #
    for test_case in test_cases:

        unit=test_case["unit"]
        case=test_case["case"]
        url=test_case["url"]

        # ------------------------------------------------------------------
        # If get-taxon-history test_case, build sub-directories for test files
        # ------------------------------------------------------------------
        if case.startswith("taxonomyHistoryRegression"):
            case_dir = build_out_dirs(args.out_dir, test_case) \
                if "bin" in test_case else args.out_dir

            ref_dir  = build_out_dirs(args.ref_dir, test_case) \
                if "bin" in test_case else args.ref_dir
            
            out_file       = os.path.join(case_dir, f"{test_case['unit']}.{test_case['case']}.txt")
            ref_file       = os.path.join(ref_dir,  f"{test_case['unit']}.{test_case['case']}.txt")

            decode_file    = os.path.join(case_dir, f"{test_case['unit']}.{test_case['case']}.decode.txt")
            ref_decode_file= os.path.join(ref_dir,  f"{test_case['unit']}.{test_case['case']}.decode.txt")

            diff_file      = os.path.join(case_dir, f"{test_case['unit']}.{test_case['case']}.decode.diff")
            dwdiff_file    = os.path.join(case_dir, f"{test_case['unit']}.{test_case['case']}.decode.dwdiff")
            dwdiff_short_file = os.path.join(case_dir,f"{test_case['unit']}.{test_case['case']}.decode.dwdiff_short")
            
        else:
            
            out_file = os.path.join(args.out_dir, f"{unit}.{case}.txt")
            ref_file = os.path.join(args.ref_dir, f"{unit}.{case}.txt")

            decode_str = "decode"
            if args.scrub:
                decode_str = "scrub"
            decode_file = os.path.join(args.out_dir, f"{unit}.{case}.{decode_str}.txt")
            ref_decode_file = os.path.join(args.ref_dir, f"{unit}.{case}.{decode_str}.txt")
            diff_file = os.path.join(args.out_dir, f"{unit}.{case}.{decode_str}.diff")
            dwdiff_file = os.path.join(args.out_dir, f"{unit}.{case}.{decode_str}.dwdiff")
            dwdiff_short_file = os.path.join(args.out_dir, f"{unit}.{case}.{decode_str}.dwdiff_short")

        # result of test case
        content = None
        result = ""
        result_line= ""
        delta_notes=""
        # do we have new decoded content to compare?
        need_compare = False
        
        # look up time from reference report.txt
        ref_key    = (unit, case)
        ref_time   = 0.0
        ref_status = ""
        delta_note = ""
        delta_note_color = ""
        pct_change_str = ""
        if ref_key in ref_timings:
            ref_time   = ref_timings[ref_key]["elapsed"]
            ref_status = ref_timings[ref_key]["status"]

        #
        # run test case, if needed
        #
        # skip cases with existing output if --update & last fetch wasn't an error
        if args.update and os.path.exists(out_file) and not file_starts_with_http(out_file):
            if args.verbose:
                print(f"# do SKIP: {ref_key}")
            result = "SKIP"
            result_color = f"{BLUE}{result}{RESET}"
            elapsed_time = ref_time
            pct_change = 0.0
        else:
            #
            # query web service, save output
            #
            if args.verbose:
                print(f"# do FETCH: {url}")
            start_time = time.time()
            content, status = fetch_url(args.server, url)
            elapsed_time = time.time() - start_time
            if args.verbose:
                print(f"# fetched {len(content)} bytes")

            save_file(out_file, content,args.verbose)
            if args.verbose:
                print(f"# saved {len(content)} bytes of result content")


        #
        # Decode REF and CONTENT, if needed
        #
        if ref_status == "HTTP":
            # ref dataset has a failed connection. Re-fail.
            summary[unit]['http'] += 1
            result = 'HTTP'
            result_color = f"{RED}{result}{RESET}"

        else:
            #
            # valid REF result to compare against
            #
            
            # REF
            if args.verbose:
                print(f"# check ref_decode_file: {ref_decode_file}")
            if os.path.exists(ref_file) and (not os.path.exists(ref_decode_file) or os.path.getmtime(ref_file) > os.path.getmtime(ref_decode_file)):
                #
                # Decode ref content (decode cache file missing)
                #
                if args.verbose:
                    print(f"# decoding REF " )

                ref_content=load_file(ref_file,args.verbose)
                if ref_content.startswith("HTTP"):
                    print(f"# ref HTTP ERROR in {ref_file}")
                    print(f"# ref HTTP ERROR: {ref_content}")
                    http_error = "REF"
                ref_decode_content = decode_unicode_escapes(ref_content, args.scrub)

                # ---- normalize get-taxon-history -----------------------------------
                try:
                    ref_json = json.loads(ref_decode_content)
                    # if unit == "taxonomy":
                    # if case.startswith("taxonomyHistoryRegression"):
                    if (unit == "taxonomy" and case.startswith("taxonomyHistoryRegression")):
                        # test
                        # print("running case code")
                        ref_json = _normalize_taxon_history(ref_json)
                    ref_decode_content = json.dumps(ref_json, separators=(",", ":"))
                except json.JSONDecodeError:
                    pass

                ref_decode_content_pretty = pretty_json_or_raw(ref_decode_content,ref_file,args.verbose,args.no_pretty)
                save_file(ref_decode_file, ref_decode_content_pretty, args.verbose)
                need_compare = True

            #
            # Decode OUT if we ran the test, or if the decode cache file is mising
            #
            if args.verbose:
                print(f"# check decode_file: {decode_file}")
            if not os.path.exists(decode_file) or os.path.getmtime(out_file) > os.path.getmtime(decode_file):
                #
                # Decode results
                #
                if args.verbose:
                    print(f"# decoding OUT " )
                if content is None:
                    content=load_file(out_file,args.verbose)
                    if args.verbose:
                        print(f"# loaded cached OUT: {len(content)} bytes from {out_file}")
                if content.startswith("HTTP"):
                    print(f"# out HTTP ERROR in {out_file}")
                    print(f"# out HTTP ERROR: {content}")
                    if http_error != "":
                        http_error = http_error+":OUT"
                    else:
                        http_error = "OUT"

                decode_content = decode_unicode_escapes(content, args.scrub)

                try:
                    data = json.loads(decode_content)
                    # if test_case["unit"] == "taxonomy":
                    # if case.startswith("taxonomyHistoryRegression"):
                    if (unit == "taxonomy" and case.startswith("taxonomyHistoryRegression")):
                        # test
                        # print("running case code")
                        data = _normalize_taxon_history(data)
                    decode_content = json.dumps(data, separators=(",", ":"))
                except json.JSONDecodeError:
                    # if the service ever returns invalid JSON we simply diff raw text
                    pass

                decode_content_pretty = pretty_json_or_raw(decode_content,decode_file,args.verbose,args.no_pretty)

                save_file(decode_file, decode_content_pretty, args.verbose)
                need_compare = True

            #
            # Compare decode OUT to REF, if anything has changed
            #

            # flag if diff output is missing
            if args.verbose:
                print(f"# check diff_file: {diff_file}")

            if not os.path.exists(diff_file) or not os.path.exists(dwdiff_file) or os.path.getmtime(out_file) > os.path.getmtime(decode_file) or os.path.getmtime(ref_decode_file) > os.path.getmtime(diff_file):
                need_compare = True
                if args.verbose:
                    print(f"# diff files missing" )

            # Do not save diff files when using --no_compare (for generating test_ref)
            if do_compare:
                if need_compare:
                    if args.verbose:
                        print(f"# running diffs " )
                    rc, diff_output   = run_diff(ref_decode_file,decode_file)
                    save_file(diff_file, diff_output, args.verbose)
                    dwdiff_output     = run_dwdiff(ref_decode_file,decode_file)
                    save_file(dwdiff_file, dwdiff_output, args.verbose)
                    dwdiff_short_output     = run_dwdiff_short(ref_decode_file,decode_file)
                    save_file(dwdiff_short_file, dwdiff_short_output, args.verbose)
                else:
                    if args.verbose:
                        print(f"# no diff needed " )
                    # check output of diff
                    if os.path.getsize(diff_file) == 0:
                        rc = 0
                    else:
                        rc = 2

                if rc == 0:
                    summary[unit]['pass'] += 1
                    result = 'PASS'
                    result_color = f"{GREEN}{result}{RESET}"
                else:
                    summary[unit]['fail'] += 1
                    result = 'FAIL'
                    result_color = f"{RED}{result}{RESET}"

            #
            # check test case run time
            #
            if ref_key in ref_timings:
                delta      = elapsed_time - ref_time
                delta_abs  = abs(delta)
                if ref_time < 0.01:
                    # void divide by 0
                    pct_change = 0.0
                else:
                    pct_change = (100.0 * delta_abs) / ref_time

                # “same speed” if within 0.005s (rounds to 0.00)
                if round(delta_abs, 2) == 0:
                    delta_note = "SAME SPEED"
                    delta_note_color=delta_note

                # slower cases only
                elif delta > 0 and pct_change >= float(args.pct_change) :
                    # print “SLOW <seconds>”
                    delta_note = f"SLOW"
                    delta_note_color = f"{ORANGE}{delta_note}{RESET}"

                if do_compare:
                    pct_change_str=f"{pct_change:+3.0f}%"


        # avoid storing 0.00 in reference timings
        if elapsed_time < 0.01:
            elapsed_time = 0.01
            
        if do_compare:
            result_line = (
                f"{elapsed_time:05.2f}s\t{pct_change_str}\t{unit}\t{case}"
                )
        else:
            result_line = f"{elapsed_time:05.2f}s\t{unit}\t{case}"

        #
        # format test case results for reporting
        #
        diff_note = ""
        if result != 'PASS' and args.show_diff:
            diff_note = dwdiff_short_file
        case_result       = f"{result}\t{result_line}\t{delta_note}\t{diff_note}"
        case_result_color = f"{result_color}\t{result_line}\t{delta_note_color}\t{diff_note}"
        detailed_results.append(case_result)
        f_report.write(case_result + "\n")
        f_report.flush()
        print(case_result_color)



    #
    # save final reports
    #
    
    # summary_path = os.path.join(args.out_dir, "report.summary.txt")
    summary_path = os.path.join(args.out_dir, args.summary_name)
    with open(summary_path, "w") as f:
        f.write(f"SERVER:{args.server}"+"\n")
        f.write(f"IN_FILE:{args.in_file}"+"\n")
        for unit, counts in summary.items():
            line = f"{unit}\tPASS: {counts['pass']}\tFAIL: {counts['fail']}\tHTTP: {counts['http']}"
            print(line)
            f.write(line + "\n")

if __name__ == "__main__":
    main()
