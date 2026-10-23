"""Run the API and the web UI: ``python -m app.cli.serve [--host H] [--port P]``."""

import argparse

import uvicorn

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000


def build_parser():
    parser = argparse.ArgumentParser(description="Serve the EpigCorpus API and web UI.")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Interface (default {DEFAULT_HOST}).")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT,
                        help=f"Port (default {DEFAULT_PORT}).")
    return parser


def main():
    args = build_parser().parse_args()
    print(f"EpigCorpus: open http://{args.host}:{args.port}/ (API under /v1)", flush=True)
    uvicorn.run("app.api.server:app", host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
