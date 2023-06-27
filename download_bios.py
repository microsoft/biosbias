from pebble import ProcessPool,  ProcessExpired
import os
from argparse import ArgumentParser
from multiprocessing import cpu_count
import time
import gzip
import json
import requests
import sys
import pickle as pkl
from warcio.archiveiterator import ArchiveIterator
import re

MAX_PAGE_LEN = 100 * 1000
MAX_LINE_LEN = 1000
MIN_LENGTH = 150 
MAX_PRECEED = 40
# PREFIXES = {'Col', 'Councillor', 'Dr', 'Lecturer', 'Maj', 'Mr', 'Mrs', 'Ms', 'Prof', 'Professor', 'Professsor'} # change back to set()

COMMON_CRAWL_URL = 'https://data.commoncrawl.org/'

parser = ArgumentParser()
parser.add_argument('wetpaths',
                    help='common_crawl date like 2017-43 (see http://commoncrawl.org/the-data/get-started/ ) *or* a path to a -wet.paths file')

# parser.add_argument("-w", "--wetpaths", dest="wetpaths",
#                     help="read paths from FILE", metavar="FILE")

parser.add_argument("-o", "--output", dest="output",
                    help="write bios to OUT.pkl", metavar="OUT")

parser.add_argument("-r", "--retries", dest="retries", type=int, default=2, help="number of retries per path")

parser.add_argument("-p", "--parallel", dest="parallel", type=int, default=0,
                    help="number of parallel threads", metavar="N")

args = parser.parse_args()

with open("freq_titles.json", "r") as f:
    freq_titles = json.load(f)

lower_freq_titles = {t.lower(): normalized for t, normalized in freq_titles.items()}


def re_escape_title(title):
    if title.isupper():
        return re.escape(title)

    return "".join(f"[{c}{c.lower()}]" if c.isupper() else re.escape(c) for c in title)


freq_titles_regex = re.compile(
    r"\b|".join(
        re_escape_title(title) for title in sorted(freq_titles)
    ) + r"\b"
)

bad_regex = re.compile(r"\b(?:I|you|your|me|my|mine|myself|our|ours|us|we|ourselves)\b", flags=re.I)
m_regex = re.compile(r"\b(?:mr|his|he|him|himself)\b", flags=re.I)
f_regex = re.compile(r"\b(?:mrs|ms|hers|she|her|herself)\b", flags=re.I)
sentence_end_regex = re.compile(r"\. +[A-Z]")


def infer_gender(bio):
    if re.search(bad_regex, bio):
        return None
    m_count = bool(re.search(m_regex, bio))
    f_count = bool(re.search(f_regex, bio))
    if f_count > 0 and m_count == 0:
        return "F"
    if m_count > 0 and f_count == 0:
        return "M"


acceptable_prefixes = "adjunct|artist|assistant|associate|attorney|author|bio|biography|brief bio|brief biography|biographical sketch|br|brother|chancellor|chaplain|chapln|col|colonel|councillor|currently|description|director|doctor|dr|experience|facilitator|father|fr|gov|governor|host|image|instructor|lecturer|madam|madame|maj|miss|missus|mister|mme|monsieur|monsignor|mr|mrs|ms|msgr|note|now|pastor|plaintiff|pres|presenter|president|prince|principal|prof|professionally|professor|profile|rabbi|reader|rep|representative|respondent|rev|reverend|reviewer|rev|saint|sen|senator|senor|senora|senorita|sgt|sir|sister|speaker|sr|sra|srta|st|the hon|the honorable|today"
lname_strip_regex = re.compile(r"^[^a-z]*(?:\b(?:[a-z]|"+ acceptable_prefixes +r")\b[^a-z]*)*", re.I) 
lname_kill_regex = re.compile(r"^(?:about|abstract|additionally|although|and|but|by|comments|example|he|plot|review|she|source|story|summary|synopsis|the|there|today|when|while|yes)\b", re.I)
rname_regex = re.compile(r"(?:[\b(,\. ]+(?:\(eae\)|[a-z]|ab|abpp|aia|ao|apn|aprn|arnp|asid|asla|ba|bs|bsn|ca|cbe|ccrn|cde|cdn|cdw|ceo|cfo|cipd|clt|cnm|cnp|cpa|cpnp|crnp|csat|cso|cssd|dc|dds|djb|dmd|dnp|e\-?ryt[\- \d]*|edd|esq|faan|facs|faia|fca|fnp|fnp-bc|fnp-c|frcs|ii|iii|iv|jd|jg|jr|lac|ladc|lcpc|lcsw|ld|ldn|licsw|ll|llm|llp|lmft|lmhc|lmt|lp|lpc|ma|mba|mc|md|mfa|mft|mlc|mms|mn|mpas|mph|ms|msn|mw|ncarb|nd|np|np-c|pa-c|pa\-c|ph|phd|pla|pm|psy|psyd|ra|rcyt[\- \d]*|rd|rdn|riba|rla|rn|rn\-bc|ryt|sr)[\b\., )]*)*$", re.I)
name_regex = re.compile(r"^([A-Z][a-zâêîôûŵŷäëïöüẅÿàèìòùẁỳáéíóúẃý]+(?:\-[A-Z][a-zâêîôûŵŷäëïöüẅÿàèìòùẁỳáéíóúẃý]+)*)( +[A-Z](?:\.|[a-zâêîôûŵŷäëïöüẅÿàèìòùẁỳáéíóúẃý]*))?((?: van)? +(?:Mc|De|O')?[A-Z][a-zâêîôûŵŷäëïöüẅÿàèìòùẁỳáéíóúẃý]+(?:\-[A-Z][a-zâêîôûŵŷäëïöüẅÿàèìòùẁỳáéíóúẃý]+)*)$")

def extract_name(name):
    name = name[lname_strip_regex.match(name).end():]
    if lname_kill_regex.match(name):
        return None
    name = name[:rname_regex.search(name).start()]
    match =  name_regex.match(name)
    if not match:
        return None
    return tuple(g.strip().replace(".", "") if g else "" for g in match.groups())

    

def log(text):
    try:
        if not text.endswith("\n"):
            text += "\n"
        with open(log_fname, "a") as f:
            f.write(text)
    except Exception as e:
        print("*** Unable to log!")
        print(e)

def extract_bios_from_page(page, URI, max_preceed=MAX_PRECEED, min_len=MIN_LENGTH):
    if "the" not in page:
        return []  # probably not English
    ISA = " is a "
    ISAN = " is an "
    n = len(ISA) + max_preceed
    matches = []
    failures = []
    for line_str in page.split('\n'):
        if len(line_str) >= min_len:
            if ISA in line_str[:n]:
                a = line_str.index(ISA)
                b = a + len(ISA)
            elif ISAN in line_str[:n + 1]:
                a = line_str.index(ISAN)
                b = a + len(ISAN)
            else:
                continue                                   
            m = re.match(freq_titles_regex, line_str[b:]) # is an architect
            if not m: # try is an American architect  (1 word before title) also catches boston-based architect
                c = line_str.find(" ", b)
                if c == -1 or line_str[c - 1] == ',': # avoid 'is a performer, architect blah ...'
                    continue
                m = re.match(freq_titles_regex, line_str[c + 1:])
                if not m:
                    continue
                end = c + 1 + m.end()
            else:
                end = b + m.end()
            if m:                    
#                 if "→" in line_str:
#                     weird = line_str.index("→")
#                     if weird<end:
#                         continue
#                     line_str = line_str[:weird]

                title = m.group()
                if title.lower() not in lower_freq_titles:
                    print(f"!!!!!! Strange title: '{title}'")
                    continue
                g = infer_gender(line_str)
                if not g or line_str[end:].startswith(",") or line_str[end:].startswith(" and "): # avoid 'is an architect and' or 'is an architect, blah' 
                    # maybe add: or line_str[end:].startswith("/") # avoid 'is an architect/designer...'
                    continue
                if line_str.find("\t", end)!=-1:
                    line_str = line_str[:line_str.find("\t", end)]
                if len(line_str) > MAX_LINE_LEN: 
                    continue
                m2 = re.search(sentence_end_regex, line_str[end:])
                if not m2:
                    continue
                start_pos = end + m2.start() + 1


                body = line_str[start_pos:].strip()
                if len(body) < min_len:
                    continue
                name = extract_name(line_str[:a])
                if not name:
                    continue
                                 
                matches.append(
                    {"raw": line_str, "name": name, "raw_title": title, "gender": g, "start_pos": start_pos,
                     "title": lower_freq_titles[title.lower()], "URI": URI})
    return matches



def dedup_exact(people):
    seen = set()
    return [p for p in people if not (p["raw"] in seen or seen.add(p["raw"]))]

def bios_from_wet_url(url, verbose=False):
    try:
        time0 = time.time()
        log("TRYING "+url)
        r = requests.get(url, stream=True)

        assert r.status_code == 200, f"*** Got status code {r.status_code} != 200"

        if verbose:
            print(f"Status code {r.status_code} for {url}")

        it = ArchiveIterator(fileobj=r.raw)
        it.__next__()

        ans = dedup_exact([bio for record in it for bio in
               extract_bios_from_page(record.content_stream().read().decode()[:MAX_PAGE_LEN], record.rec_headers.get_header('WARC-Target-URI'))])
        log(f"DONE {url} {time.time()-time0:.1f} seconds")
        return ans

    except Exception as e:
        print(f"*** Exception in {url}:", file=sys.stderr)
        print(f"*** {e}", file=sys.stderr)
        print(f"***", file=sys.stderr)
        print("", file=sys.stderr)
        return None


def chunks(arr, n):
    n = max(n, 1)
    m = len(arr)
    return [arr[(m * i) // n:(m * (i + 1)) // n] for i in range(n)]


def process_urls(paths, n_processes, prefix=COMMON_CRAWL_URL, max_failures=100, num_progress_reports=50):
    print(f"Using {n_processes} parallel processes")
    failed_paths = []
    bios = []
    time0 = time.time()
    path_name = (paths[0] + '///').split('/')[1]
    num_progress_reports = max(1, min(num_progress_reports, len(paths) // n_processes))
    done = 0
    pool = ProcessPool(n_processes)
    for i, paths_chunk in enumerate(chunks(paths, num_progress_reports)):
        ans = pool.map(bios_from_wet_url, [prefix + path for path in paths_chunk], timeout=1200)
        iterator = ans.result()
        for p in paths_chunk + ["done"]:
            try:
                a = next(iterator)
                assert p != "done"
                if a is not None:
                    bios += [dict(path=p, **b) for b in a]
                    continue
            except StopIteration:
                assert p == "done"
                break
            except Exception as error:
                print("--------------------\n"*10 + f"function raised {error}")
            failed_paths.append(p) 
            
        done += len(paths_chunk)
        pct = (i + 1) / num_progress_reports
        eta = (time.time() - time0) * (1 / pct - 1) / 60 / 60
        print(
            f"{eta:.1f} hours left, {done:,}/{len(paths):,} done ({pct:.0%}),",
            f"{int(len(bios)/pct):,} estimated bios, {path_name}"
        )
        if len(failed_paths) > 0:
            print(f" {len(failed_paths):,} failed paths")
            if len(failed_paths) > max_failures:
                break
    pool.close()
    return dedup_exact(bios), failed_paths # dedup_exact is new!


if __name__ == "__main__":
    if not args.wetpaths.endswith("wet.paths"):
        assert re.match(r"^[0-9]+-[0-9]+$",
                        args.wetpaths), "Expecting wetpaths to be either xxxx-xx or to end with wet.paths"
        url = COMMON_CRAWL_URL + "crawl-data/CC-MAIN-{}/wet.paths.gz".format(args.wetpaths)
        r = requests.get(url, stream=True)
        assert r.status_code == 200
        r.raw.decode_content = True  # just in case transport encoding was applied
        gzip_file = gzip.GzipFile(fileobj=r.raw)
        paths = [line.decode().strip() for line in gzip_file]
        print("Got {:,} urls from {}".format(len(paths), url))
        path_root = "CC-MAIN-{}-".format(args.wetpaths)
    else:
        with open(args.wetpaths, "r") as f:
            paths = [x.strip() for x in f.readlines()]
        path_root = args.wetpaths.replace("wet.paths", "")

    paths = paths

    if args.parallel == 0:
        c = cpu_count()
        args.parallel = c-c//10

    output_fname = args.output or (path_root + "bios.pkl")

    assert output_fname.endswith("bios.pkl"), "Output filename must end with 'bios.pkl'"

    log_fname = output_fname.replace("bios.pkl", "log.txt")

    try:
        os.remove(log_fname)
    except:
        pass

    bios, failed_paths = process_urls(paths, n_processes=args.parallel)
    if len(failed_paths) < len(paths) / 10:
        for i in range(args.retries):
            if not failed_paths:
                break
            print("\n" * 5)
            print(f"*** Retry #{i+1} with {len(failed_paths)} failures")
            more_bios, failed_paths = process_urls(failed_paths, n_processes=args.parallel)
            bios += more_bios

    with open(output_fname, "wb") as f:
        print(f"Wrote {len(bios):,} bios to {output_fname}")
        pkl.dump(bios, f)

    if len(failed_paths) > 0:
        log("\nFailed paths:\n" + "\n".join(failed_paths))
        print(f"*** Wrote {len(failed_paths):,} failures to {log_fname}")

