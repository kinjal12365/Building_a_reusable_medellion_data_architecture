import sys
sys.path.insert(0, 'src')
from pathlib import Path
from bronze.discovery.file_lister import list_files

# Create temp test files in tests/fixtures/
fixtures = Path("tests/fixtures")
fixtures.mkdir(parents=True, exist_ok=True)

(fixtures / "sample1.csv").write_text("id,name\n1,Alice\n2,Bob")
(fixtures / "sample2.json").write_text('{"id": 1, "name": "Alice"}')
(fixtures / "sample3.csv").write_text("id,name\n3,Charlie")

# List all files
files = list_files(str(fixtures), pattern="*")
print(f"Found {len(files)} files:")
for f in files:
    print(f"  {f.file_name} | hash: {f.content_hash[:12]}... | size: {f.size_bytes} bytes")

# List only CSVs
csv_files = list_files(str(fixtures), pattern="*.csv")
print(f"\nCSV only: {[f.file_name for f in csv_files]}")

print("\nFile lister works!")