"""Break a machine on purpose.

    python scripts/inject_fault.py --machine 7 --type spike --seconds 30
    python scripts/inject_fault.py --clear

Writes runtime/faults.json, which the simulator reads before every simulated hour.
"""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

FAULT_FILE = Path(__file__).resolve().parents[1] / "runtime" / "faults.json"
TYPES = ["spike", "drift", "malformed", "late", "duplicate"]


def stamp(ts: datetime) -> str:
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--machine", type=int, help="machine number, 1 to 100")
    parser.add_argument("--type", choices=TYPES)
    parser.add_argument("--seconds", type=int, default=30, help="how long the fault lasts")
    parser.add_argument("--clear", action="store_true", help="remove all faults")
    args = parser.parse_args()
    if not args.clear and (args.machine is None or args.type is None):
        parser.error("give --machine and --type, or --clear")

    now = datetime.now(UTC)
    FAULT_FILE.parent.mkdir(exist_ok=True)
    faults = []
    if not args.clear:
        if FAULT_FILE.exists():
            faults = json.loads(FAULT_FILE.read_text())
        faults = [f for f in faults if f["until"] > stamp(now) and f["machine_id"] != args.machine]
        until = stamp(now + timedelta(seconds=args.seconds))
        faults.append({"machine_id": args.machine, "type": args.type, "until": until})
        print(f"fault active: machine {args.machine}, {args.type}, until {until}")
    else:
        print("all faults cleared")
    FAULT_FILE.write_text(json.dumps(faults, indent=2))


if __name__ == "__main__":
    main()
