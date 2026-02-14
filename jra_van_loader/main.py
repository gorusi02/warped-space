import sys
import argparse
from jvlink.client import JVLinkClient
from storage import DataSaver


def main():
    parser = argparse.ArgumentParser(description="JRA-VAN Data Loader")
    parser.add_argument("--spec", required=True, help="Data Specification (e.g., RACE, diff)")
    parser.add_argument("--from", dest="from_time", default="20240101000000", help="From Time (YYYYMMDDHHMMSS)")
    parser.add_argument("--option", type=int, default=1, help="JVOpen Option (1:Normal, 2:Setup, 4:Update)")
    parser.add_argument("--output", default="output_data", help="Output directory")

    args = parser.parse_args()

    # Windows console output encoding
    sys.stdout.reconfigure(encoding="utf-8")

    print("=== JRA-VAN Loader Start ===")
    print(f"Spec: {args.spec}, From: {args.from_time}")

    saver = DataSaver(output_dir=args.output)

    try:
        with JVLinkClient() as client:
            client.open(args.spec, args.from_time, args.option)

            for line in client.read():
                if line:
                    saver.save(line)

    except Exception as e:
        print(f"[ERROR] Failed: {e}")
        import traceback

        traceback.print_exc()
    finally:
        saver.close()


if __name__ == "__main__":
    main()
