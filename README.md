# ENKK orvoskereso scraper

Ez a repository egy Playwright alapú scraper scriptet tartalmaz a `https://kereso.enkk.hu/` oldalhoz.

## Mit kezel

- A keresest csak nevmezon keresztul vegzi.
- A 100-as talalati limitet prefix alapu bontassal probalja meg feloldani.
- A csuszkat eloszor automatan probalja elhuzni, sikertelenseg eseten manualis fallback van.
- Deduplikalt kimenetet ment `JSON` es `CSV` formatumban.
- Minden talalathoz megprobal PDF-et menteni az adatlaprol (`Adatlap megtekintese` -> print/PDF).
- Az oldalbol generalt PDF fallback alapbol KI van kapcsolva (`--pdf-fallback-page-pdf` kapcsoloval kerheto).
 - Alapertelmezetten csak adatbazis-kimenet keszul (JSON/CSV), PDF letoltes kulon kapcsolhato.

## Telepites

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

## Futtatas

```bash
python scraper_enkk.py
```

Alapertelmezett kimenet:

- `data/orvosok.json`
- `data/orvosok.csv`
- `data/overflow_terms.json` (ha marad fel nem oldott 100-as csoport)

Egybetus gyors teszt:

```bash
python scraper_enkk.py --alphabet a --max-depth 1
```

PDF letoltes bekapcsolasa:

```bash
python scraper_enkk.py --download-pdfs
```

## Hasznos opciok

```bash
python scraper_enkk.py \
  --max-depth 4 \
  --split-threshold 100 \
  --query-delay-ms 1200 \
  --results-wait-ms 7000 \
  --manual-slider-attempts 4 \
  --manual-slider-wait-ms 3500 \
  --download-pdfs \
  --max-pdfs-per-query 30 \
  --pdf-fallback-page-pdf \
  --record-type "Orvos/fogorvos" \
  --no-force-submit \
  --pdf-dir data/pdfs
```

## Megjegyzes

Mivel az oldal anti-bot csuszkat hasznal, teljesen hands-off futas nem mindig varhato. A script ugy van irva, hogy ha az automata slider mozgatas nem eleg, a terminalban ker Entert manualis sliderhuzas utan.
