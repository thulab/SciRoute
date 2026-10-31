"""File-only corpus, query case, run and historical evidence identities."""
import argparse
import json
from pathlib import Path
import sys

from .importers import import_multi_file, import_route3
from .model import build_case, build_run, require_comparable, verify
from .registry import Registry


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError(message)


def _unique(pairs):
    result = {}
    for key,value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _load(path):
    source = Path(path)
    if not source.is_file() or source.is_symlink() or source.stat().st_size > 256*1024*1024:
        raise ValueError("input must be a bounded regular nonsymlink JSON file")
    def nonfinite(_):
        raise ValueError("nonfinite JSON number")
    result = json.loads(source.read_bytes(), object_pairs_hook=_unique, parse_constant=nonfinite)
    if not isinstance(result, dict):
        raise ValueError("input document must be a JSON object")
    return result


def parser():
    root = Parser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True, parser_class=Parser)
    def command(name, help_text):
        item = commands.add_parser(name, help=help_text)
        item.add_argument("--registry", type=Path, required=True)
        return item
    item = command("import-corpus", "Import complete existing manifests; missing evidence records an unbound artifact")
    item.add_argument("--format", dest="import_format", choices=("multi-file", "route3"), required=True)
    item.add_argument("--manifest", type=Path, required=True)
    item.add_argument("--expected-sha256", required=True)
    item.add_argument("--publication", type=Path)
    item = command("case", "Freeze one explicit query case against an independently published corpus")
    item.add_argument("--corpus-id", required=True)
    item.add_argument("--request", type=Path, required=True)
    item = command("run", "Register an explicit run identity document; does not execute it")
    item.add_argument("--document", type=Path, required=True)
    item = command("compare", "Require matching scientific and measurement identities for an A/B pair")
    item.add_argument("--left", required=True)
    item.add_argument("--right", required=True)
    item = command("show", "Read and verify a registered corpus, case or run")
    item.add_argument("--kind", choices=("corpus", "case", "run"), required=True)
    item.add_argument("--id", required=True)
    item = command("verify-inputs", "Read raw source bytes and verify them against a registered corpus")
    item.add_argument("--corpus-id", required=True)
    item.add_argument("--raw-root", type=Path, required=True)
    for name,help_text in (("map-history", "Attach an explicit historical row mapping to a case; never rewrite results"),
                           ("unbound", "Preserve evidence lacking the required identity proof")):
        item = command(name, help_text)
        item.add_argument("--document", type=Path, required=True)
    return root


def execute(args):
    registry = Registry(args.registry)
    if args.command == "import-corpus":
        publication = _load(args.publication) if args.publication is not None else None
        importer = import_multi_file if args.import_format == "multi-file" else import_route3
        imported = importer(args.manifest, expected_sha256=args.expected_sha256,
                            publication_evidence=publication)
        if imported["bound"] is not True:
            return registry.register_unbound({"bound":False,
                "reason":"corpus import lacks complete binding evidence: "+"; ".join(imported["warnings"]),
                "provenance":{"manifest":str(args.manifest),"imported":imported}})
        from .model import build_corpus
        return registry.register_corpus(build_corpus(imported))
    if args.command == "case":
        corpus = registry.get_corpus(args.corpus_id)
        return registry.register_case(build_case(corpus, _load(args.request)))
    if args.command == "run":
        document = _load(args.document)
        if "run_id" in document:
            verify(document, "run")
            return registry.register_run(document)
        required = {"case_id","execution","arm","implementation","clock","cache","environment","replication"}
        if set(document) != required:
            raise ValueError("unsealed run requires exact case/execution/arm/implementation/clock/cache/environment/replication fields")
        case = registry.get_case(document["case_id"])
        return registry.register_run(build_run(case, **{key:value for key,value in document.items() if key != "case_id"}))
    if args.command == "compare":
        left,right = registry.get_run(args.left),registry.get_run(args.right)
        require_comparable(left,right)
        return {"comparable":True,"left":args.left,"right":args.right,
                "corpus_id":left["corpus_id"],"case_id":left["case_id"]}
    if args.command == "show":
        return {"corpus":registry.get_corpus,"case":registry.get_case,"run":registry.get_run}[args.kind](args.id)
    if args.command == "verify-inputs":
        return registry.verify_raw(args.corpus_id,args.raw_root)
    if args.command == "map-history":
        return registry.register_mapping(_load(args.document))
    if args.command == "unbound":
        return registry.register_unbound(_load(args.document))
    raise ValueError("unsupported command")


def main(argv=None):
    try:
        result = execute(parser().parse_args(argv))
        print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))
        return 0
    except (ValueError,OSError,KeyError,TypeError) as error:
        print(json.dumps({"complete":False,"error":str(error)}, sort_keys=True, allow_nan=False))
        return 2


if __name__ == "__main__":
    sys.exit(main())
