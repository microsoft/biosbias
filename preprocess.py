import random, glob, re
import pickle as pkl
from argparse import ArgumentParser

titles_to_ignore = {'real_estate_broker', 'landscape_architect', 'massage_therapist', 'magician', 'acupuncturist'} # close but not enough data on these titles :-(

def save_pkl(obj, filename):
    with open(filename, "wb") as f:
        pkl.dump(obj, f)
        
def load_pkl(filename, verbose=True):
    if verbose:
        print(f"Loading '{filename}'")
    with open(filename, "rb") as f:
        return pkl.load(f)


def process(p, replacement="_"):
    bio = p["raw"][p["start_pos"]:].strip()
    names = p["name"] 
    assert len(names)==3

    regExp = r"\b(?:[Hh]e|[Ss]he|[Hh]er|[Hh]is|[Hh]im|[Hh]ers|[Hh]imself|[Hh]erself|[Mm][Rr]|[Mm][Rr][sS]|[Mm][Ss]|"
    regExp += "|".join([re.escape(n) for n in names if len(n)>0]) + r")\b"
    
    bio = re.sub(regExp, replacement, bio)
        
    p["bio"]=bio

def group_by(l, func):
    ans = {}
    for i in l:
        k = func(i)
        if k not in ans:
            ans[k] = [i]
        else:
            ans[k].append(i)
    return ans

def dedup_middle(bios): # remove triples where the middle name is a prefix of another middle name, so {Mary Lynn Doe, Mary L Doe, Mary Doe} => {Mary Lynn Doe}, but {Mary L Doe, Mary I Doe} => {Mary L Doe, Mary I Doe}
    trips = group_by(bios, lambda b: (b["title"], b["name"][0], b["name"][2]))
    for k in trips:
        to_remove = set()
        if len(k)==1:
            continue
        for b1 in trips[k]:
            for b2 in trips[k]:
                if b1 is not b2 and b1["name"][1].startswith(b2["name"][1]):
                    to_remove.add(b2["name"][1])
        if to_remove:
            trips[k] = [b for b in trips[k] if b["name"][1] not in to_remove]
    return [b for v in trips.values() for b in v]

def dedup(people): 
    by_name_title = group_by(people, lambda b: (b["name"], b["title"]))
    ans = [sorted(ps, key = lambda p: (-len(p["raw"]), p["raw"], p["path"]))[0] for ps in by_name_title.values()]
    return ans

def main(paths, output_filename):
    all_people = [p for path in paths for p in load_pkl(path) if p["title"] not in titles_to_ignore]
    people = dedup_middle(dedup(all_people))
    print(f"{len(people):,}/{len(all_people):,} 'different' name+titles ({len(people)/len(all_people):.1%})")
    print("Processing bios...")
    for p in people:
        process(p)
    save_pkl(people, output_filename)
    print(f"Wrote {len(people):,} bios to '{output_filename}'")
    #if len(peoples)>1: # show overlaps
    #    name_titles = [{(p["name"][0], p["name"][1], p["title"]) for p in people} for people in peoples]
    #    for path1, nts1 in zip(args.paths, name_titles):
    #        for path2, nts2 in zip(args.paths, name_titles):
    #            if path1<path2:
    #                overlap2 = sum([nt in nts2 for nt in nts1])/len(nts1) + sum([nt in nts1 for nt in nts2])/len(nts2)
    #                print(f"{overlap2/2:.1%} overlap between {path1:20} and {path2:20}")
    #    output = dedup([p for ps in peoples for p in ps])
    #else:
    #    assert len(peoples)==1
    #    output = peoples[0]

if __name__ == '__main__':
    parser = ArgumentParser(description='Dedup bios by name + title and add name field to records.')
    parser.add_argument('paths', nargs='+', help='Path of bios .pkl file(s)', metavar="PATH")
    parser.add_argument("-o", "--output", dest="output", help="write bios to OUT.pkl", metavar="OUT", default="BIOS.pkl")
    args = parser.parse_args()
    main(args.paths, args.output)
