"""Download the fixed milestone files from NYC TLC and record their provenance."""

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
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = []
    for source in FILES:
        url = f"{BASE_URL}/{source}"
        target = args.output / Path(source).name
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
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
