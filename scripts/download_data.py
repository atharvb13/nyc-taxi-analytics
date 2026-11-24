"""Download the four TLC files and record their checksums."""

import argparse
import hashlib
import json
import shutil
from datetime import date
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.request import urlopen

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
FILES = (
    "trip-data/yellow_tripdata_2024-01.parquet",
    "trip-data/yellow_tripdata_2024-02.parquet",
    "trip-data/yellow_tripdata_2024-03.parquet",
    "trip-data/yellow_tripdata_2024-04.parquet",
    "misc/taxi_zone_lookup.csv",
)


def download(url: str, target: Path) -> None:
    temporary = None
    try:
        with NamedTemporaryFile(dir=target.parent, delete=False) as output:
            temporary = Path(output.name)
            with urlopen(url, timeout=60) as response:
                shutil.copyfileobj(response, output)
        if target.suffix == ".parquet":
            with temporary.open("rb") as file:
                if file.read(4) != b"PAR1":
                    raise ValueError(f"{url} did not return a Parquet file")
                file.seek(-4, 2)
                if file.read(4) != b"PAR1":
                    raise ValueError(f"{url} returned an incomplete Parquet file")
        temporary.replace(target)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    parser.add_argument("--refresh", action="store_true", help="download even if the checksum matches")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest_path = args.output / "manifest.json"
    previous = {}
    if manifest_path.is_file() and not args.refresh:
        previous = {
            entry["filename"]: entry for entry in json.loads(manifest_path.read_text(encoding="utf-8"))
        }

    manifest = []
    for source in FILES:
        url = f"{BASE_URL}/{source}"
        target = args.output / Path(source).name
        entry = previous.get(target.name)
        if entry and entry["url"] == url and target.is_file() and target.stat().st_size == entry["bytes"]:
            with target.open("rb") as file:
                if hashlib.file_digest(file, "sha256").hexdigest() == entry["sha256"]:
                    print(f"Verified {target.name}", flush=True)
                    manifest.append(entry)
                    continue
        print(f"Downloading {target.name}", flush=True)
        download(url, target)
        with target.open("rb") as file:
            manifest.append({
                "filename": target.name,
                "url": url,
                "downloaded_on": date.today().isoformat(),
                "bytes": target.stat().st_size,
                "sha256": hashlib.file_digest(file, "sha256").hexdigest(),
            })
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
