#!/usr/bin/env python3
"""Entry point for the daily business KPI report.

Usage:
  python run_report.py                              # Run for J-1
  python run_report.py --date 2026-04-15            # Run for specific date
  python run_report.py --backfill --from 2026-04-01 --to 2026-04-15
  python run_report.py --dry-run                    # Don't post to Slack
"""
import argparse
import logging
import sys
import datetime

import config


def setup_logging():
    level = getattr(logging, config.LOG_LEVEL.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def date_range(start: str, end: str):
    """Yield YYYY-MM-DD strings from start to end inclusive."""
    current = datetime.date.fromisoformat(start)
    end_date = datetime.date.fromisoformat(end)
    while current <= end_date:
        yield current.isoformat()
        current += datetime.timedelta(days=1)


def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="GOWOD Daily Business KPI Report")
    parser.add_argument("--date", help="Report date (YYYY-MM-DD). Defaults to J-1.")
    parser.add_argument("--backfill", action="store_true", help="Backfill mode.")
    parser.add_argument("--from", dest="from_date", help="Backfill start date (YYYY-MM-DD).")
    parser.add_argument("--to", dest="to_date", help="Backfill end date (YYYY-MM-DD).")
    parser.add_argument("--dry-run", action="store_true", help="Print Slack message, don't post.")
    args = parser.parse_args()

    from pipeline import run_pipeline

    if args.backfill:
        if not args.from_date or not args.to_date:
            parser.error("--backfill requires --from and --to")
        dates = list(date_range(args.from_date, args.to_date))
        logging.getLogger(__name__).info(f"Backfill: {len(dates)} dates from {args.from_date} to {args.to_date}")
        failed = []
        for d in dates:
            ok = run_pipeline(d, dry_run=args.dry_run)
            if not ok:
                failed.append(d)
        if failed:
            logging.getLogger(__name__).error(f"Failed dates: {failed}")
            sys.exit(1)
    else:
        if args.date:
            report_date = args.date
        else:
            report_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()

        ok = run_pipeline(report_date, dry_run=args.dry_run)
        if not ok:
            sys.exit(1)


if __name__ == "__main__":
    main()
