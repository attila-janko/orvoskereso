#!/usr/bin/env python3
import argparse
import asyncio
import csv
import hashlib
import json
import re
from collections import deque
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

try:
    from playwright.async_api import Error as PlaywrightError
    from playwright.async_api import Page, async_playwright
    from playwright.async_api import TimeoutError as PlaywrightTimeoutError
except ModuleNotFoundError:  # pragma: no cover - runtime dependency guard
    PlaywrightError = Exception  # type: ignore[assignment]
    PlaywrightTimeoutError = Exception  # type: ignore[assignment]
    Page = Any  # type: ignore[misc,assignment]
    async_playwright = None


NAME_INPUT_SELECTORS = [
    'input[name*="nev" i]',
    'input[id*="nev" i]',
    'input[placeholder*="Név" i]',
    'input[placeholder*="nev" i]',
    "input[type='text']",
]

SEARCH_BUTTON_SELECTORS = [
    "button:has-text('Keresés')",
    "input[type='submit'][value*='Keres']",
    "input[type='button'][value*='Keres']",
    "button[type='submit']",
]

SLIDER_SELECTORS = [
    ".ui-slider-handle",
    ".slider-handle",
    "[role='slider']",
    ".noUi-handle",
]

DETAIL_LINK_TEXTS = ["Adatlap megtekintése", "Adatlap megtekintese"]


def normalize(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


async def first_visible(page: Page, selectors: list[str]):
    for selector in selectors:
        locator = page.locator(selector)
        count = await locator.count()
        for idx in range(count):
            candidate = locator.nth(idx)
            try:
                if await candidate.is_visible():
                    return candidate
            except PlaywrightError:
                continue
    return None


async def first_visible_enabled(page: Page, selectors: list[str]):
    for selector in selectors:
        locator = page.locator(selector)
        count = await locator.count()
        for idx in range(count):
            candidate = locator.nth(idx)
            try:
                if await candidate.is_visible() and await candidate.is_enabled():
                    return candidate
            except PlaywrightError:
                continue
    return None


async def fill_name(page: Page, name_fragment: str) -> None:
    field = await first_visible(page, NAME_INPUT_SELECTORS)
    if field is None:
        raise RuntimeError("Nem talaltam nevet fogado input mezot.")
    await field.click()
    await field.fill(name_fragment)


async def trigger_name_input_events(page: Page) -> None:
    field = await first_visible(page, NAME_INPUT_SELECTORS)
    if field is None:
        return
    await field.evaluate(
        """
        (el) => {
          el.dispatchEvent(new Event('input', { bubbles: true }));
          el.dispatchEvent(new Event('change', { bubbles: true }));
          el.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true, key: 'a' }));
        }
        """
    )


async def maybe_select_record_type(page: Page, wanted: str | None) -> None:
    if not wanted:
        return

    wanted_norm = wanted.casefold()
    selects = await page.query_selector_all("select")
    for select in selects:
        options = await select.query_selector_all("option")
        for option in options:
            label = normalize(await option.inner_text())
            if wanted_norm in label.casefold():
                value = await option.get_attribute("value")
                if value is not None:
                    await select.select_option(value=value)
                else:
                    await select.select_option(label=label)
                return


async def wait_for_search_enabled(page: Page, timeout_ms: int) -> bool:
    elapsed = 0
    step = 200
    while elapsed < timeout_ms:
        if await is_search_button_enabled(page):
            return True
        await page.wait_for_timeout(step)
        elapsed += step
    return await is_search_button_enabled(page)


async def force_submit_search(page: Page) -> bool:
    return bool(
        await page.evaluate(
            """
            () => {
              const button = document.querySelector("button[type='submit'], input[type='submit'][value*='Keres']");
              const nameInput = document.querySelector("input[name*='nev' i], input[id*='nev' i], input[type='text']");
              const form = (button && button.form) || (nameInput && nameInput.form) || document.querySelector('form');
              if (!form) return false;
              try {
                if (typeof form.requestSubmit === 'function') {
                  form.requestSubmit();
                  return true;
                }
                form.submit();
                return true;
              } catch (_err) {
                return false;
              }
            }
            """
        )
    )


async def click_search(page: Page, allow_force_submit: bool) -> None:
    button = await first_visible_enabled(page, SEARCH_BUTTON_SELECTORS)
    if button is not None:
        await button.click(timeout=2500)
        return

    if allow_force_submit and await force_submit_search(page):
        return

    debug = await page.evaluate(
        """
        () => {
          const b = document.querySelector("#ok, input[type='submit'][value*='Keres'], button[type='submit']");
          if (!b) return "search button not found";
          const disabled = b.hasAttribute('disabled') || b.disabled;
          return `search button disabled=${disabled}`;
        }
        """
    )
    raise RuntimeError(f"Nem sikerult Kereses submit. Allapot: {debug}")


async def is_search_button_enabled(page: Page) -> bool:
    button = await first_visible_enabled(page, SEARCH_BUTTON_SELECTORS)
    if button is None:
        return False
    try:
        return await button.is_enabled()
    except PlaywrightError:
        return False


async def try_auto_slider(page: Page, timeout_ms: int) -> bool:
    for selector in SLIDER_SELECTORS:
        handle = page.locator(selector).first
        if await handle.count() == 0:
            continue
        if not await handle.is_visible():
            continue

        box = await handle.bounding_box()
        if not box:
            continue

        start_x = box["x"] + box["width"] / 2
        start_y = box["y"] + box["height"] / 2

        # Multiple distances improve success chance across different slider widths.
        for delta in (180, 240, 300, 360):
            await page.mouse.move(start_x, start_y)
            await page.mouse.down()
            await page.mouse.move(start_x + delta, start_y, steps=24)
            await page.mouse.up()
            await page.wait_for_timeout(250)

            if await is_search_button_enabled(page):
                return True

    # Sometimes slider disappears after success, even when button state check fails.
    await page.wait_for_timeout(timeout_ms)
    return await is_search_button_enabled(page)


async def wait_for_manual_slider(term: str) -> None:
    prompt = (
        f"\n[{term}] Huzd el a csuszkat a bongeszoben, majd nyomj Enter-t itt a folytatashoz..."
    )
    await asyncio.to_thread(input, prompt)


async def wait_for_results(page: Page, timeout_ms: int) -> None:
    elapsed = 0
    step = 300
    while elapsed < timeout_ms:
        ready = await page.evaluate(
            """
            () => {
              const body = document.body ? document.body.innerText : '';
              if (/nincs talalat/i.test(body)) return true;
              const tables = Array.from(document.querySelectorAll('table'));
              for (const table of tables) {
                const rows = table.querySelectorAll('tbody tr, tr');
                if (rows.length > 0) return true;
              }
              return false;
            }
            """
        )
        if ready:
            return
        await page.wait_for_timeout(step)
        elapsed += step


async def extract_rows(page: Page) -> dict[str, Any]:
    return await page.evaluate(
        """
        () => {
          const normalize = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const visibleTables = Array.from(document.querySelectorAll('table'))
            .filter((t) => t.offsetParent !== null);

          let best = null;
          let bestRows = [];
          for (const table of visibleTables) {
            const tbodyRows = Array.from(table.querySelectorAll('tbody tr'));
            const allRows = tbodyRows.length > 0 ? tbodyRows : Array.from(table.querySelectorAll('tr'));
            if (allRows.length > bestRows.length) {
              best = table;
              bestRows = allRows;
            }
          }

          const bodyText = normalize(document.body ? document.body.innerText : '');
          let totalHits = null;

          let match = bodyText.match(/talalatok\\s*szama\\s*:?\\s*(\\d+)/i);
          if (!match) match = bodyText.match(/(\\d+)\\s*talalat/i);
          if (match) totalHits = Number(match[1]);

          if (!best || bestRows.length === 0) {
            return { rows: [], rowCount: 0, totalHits, rawText: bodyText };
          }

          const headerCells = Array.from(best.querySelectorAll('thead th'));
          let headers = headerCells.map((h) => normalize(h.innerText));
          if (headers.length === 0) {
            const first = bestRows[0];
            if (first) {
              const maybeHeader = Array.from(first.querySelectorAll('th'));
              if (maybeHeader.length > 0) {
                headers = maybeHeader.map((h) => normalize(h.innerText));
                bestRows = bestRows.slice(1);
              }
            }
          }

          const rows = [];
          bestRows.forEach((row, rowIndex) => {
            const cells = Array.from(row.querySelectorAll('td, th'));
            if (cells.length === 0) return;

            const record = {};
            cells.forEach((cell, idx) => {
              const key = headers[idx] || `col_${idx + 1}`;
              record[key] = normalize(cell.innerText);
            });

            const clickables = Array.from(
              row.querySelectorAll("a, button, input[type='button'], input[type='submit'], [onclick]")
            );
            let detailLink = null;
            for (const link of clickables) {
              const txt = normalize(link.innerText || link.value || link.title).toLowerCase();
              const href = normalize(link.getAttribute('href')).toLowerCase();
              const onclick = normalize(link.getAttribute('onclick')).toLowerCase();
              if (
                txt.includes('adatlap') ||
                href.includes('adatlap') ||
                href.includes('pdf') ||
                onclick.includes('adatlap') ||
                onclick.includes('pdf') ||
                txt.includes('megtekint')
              ) {
                detailLink = link;
                break;
              }
            }
            if (detailLink) {
              const href = normalize(detailLink.getAttribute('href'));
              const onclick = normalize(detailLink.getAttribute('onclick'));
              record._detail_text = normalize(detailLink.innerText || detailLink.value || detailLink.title);
              record._detail_href = href;
              record._detail_onclick = onclick;
              if (href && !href.toLowerCase().startsWith('javascript:')) {
                try {
                  record._detail_url = new URL(href, window.location.href).href;
                } catch (_err) {
                  record._detail_url = href;
                }
              } else {
                record._detail_url = '';
              }
            } else {
              record._detail_text = '';
              record._detail_href = '';
              record._detail_onclick = '';
              record._detail_url = '';
            }
            record._row_index = rowIndex + 1;

            if (Object.values(record).some((v) => v !== '')) rows.push(record);
          });

          return { rows, rowCount: rows.length, totalHits, rawText: bodyText };
        }
        """
    )


def row_key(row: dict[str, Any]) -> str:
    key_markers = [
        "nyilvantartasi",
        "nyilvántartási",
        "pecsetszam",
        "pecsétszám",
        "azonosito",
        "azonosító",
    ]
    for k, v in row.items():
        if k.startswith("_"):
            continue
        kl = k.casefold()
        if any(marker in kl for marker in key_markers):
            val = normalize(v)
            if val:
                return f"id:{val}"

    name_markers = ["nev", "név"]
    name = ""
    for k, v in row.items():
        if k.startswith("_"):
            continue
        if any(marker in k.casefold() for marker in name_markers):
            name = normalize(v)
            if name:
                break
    canonical = {k: v for k, v in row.items() if not k.startswith("_")}
    return f"name:{name}|row:{json.dumps(canonical, sort_keys=True, ensure_ascii=False)}"


def first_row_value_by_markers(row: dict[str, Any], markers: list[str]) -> str:
    for key, value in row.items():
        key_norm = key.casefold()
        if any(marker in key_norm for marker in markers):
            val = normalize(value)
            if val:
                return val
    return ""


def sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^\w\-.]+", "_", value, flags=re.UNICODE).strip("._")
    return cleaned[:90] or "unknown"


def extract_url_from_js(js_code: str, base_url: str) -> str:
    if not js_code:
        return ""
    candidates = re.findall(r"""['"]([^'"]+)['"]""", js_code)
    for token in candidates:
        token_norm = token.strip()
        if not token_norm:
            continue
        if token_norm in {"/", "#"}:
            continue
        if token_norm.startswith(("http://", "https://", "/")):
            resolved = urljoin(base_url, token_norm)
            if resolved.rstrip("/") == base_url.rstrip("/"):
                continue
            return resolved
        if any(fragment in token_norm.casefold() for fragment in ("adatlap", "pdf", "print")):
            return urljoin(base_url, token_norm)
    return ""


def is_useless_detail_url(url: str, base_url: str) -> bool:
    url_norm = normalize(url)
    if not url_norm:
        return True
    if url_norm in {"/", "#"}:
        return True
    resolved = urljoin(base_url, url_norm)
    if resolved.rstrip("/") == base_url.rstrip("/"):
        return True
    return False


def detail_url_from_row(row: dict[str, Any], base_url: str) -> str:
    direct = normalize(row.get("_detail_url"))
    if direct and not is_useless_detail_url(direct, base_url):
        return direct

    href = normalize(row.get("_detail_href"))
    if href and not href.casefold().startswith("javascript:") and not is_useless_detail_url(
        href, base_url
    ):
        return urljoin(base_url, href)

    onclick = normalize(row.get("_detail_onclick"))
    parsed = extract_url_from_js(onclick, base_url)
    if parsed:
        return parsed
    return ""


def build_pdf_path(output_dir: Path, row: dict[str, Any], detail_url: str) -> Path:
    reg_id = first_row_value_by_markers(
        row,
        ["nyilvantartasi", "nyilvántartási", "pecsetszam", "pecsétszám", "azonosito", "azonosító"],
    )
    doc_name = first_row_value_by_markers(row, ["nev", "név"])
    seed = row_key(row) + "|" + detail_url
    digest = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:10]
    file_name = f"{sanitize_filename(reg_id or 'id')}_{sanitize_filename(doc_name or 'orvos')}_{digest}.pdf"
    return output_dir / file_name


async def discover_pdf_links(detail_page: Page) -> list[str]:
    return await detail_page.evaluate(
        """
        () => {
          const urls = [];
          const pushUrl = (url) => {
            if (!url) return;
            try {
              urls.push(new URL(url, window.location.href).href);
            } catch (_err) {
            }
          };

          const selectors = [
            "a[href]",
            "iframe[src]",
            "embed[src]",
            "object[data]"
          ];

          for (const selector of selectors) {
            const nodes = Array.from(document.querySelectorAll(selector));
            for (const node of nodes) {
              const raw = node.getAttribute("href") || node.getAttribute("src") || node.getAttribute("data") || "";
              if (!raw) continue;
              const low = raw.toLowerCase();
              if (low.includes(".pdf") || low.includes("pdf") || low.includes("print")) {
                pushUrl(raw);
              }
            }
          }

          return Array.from(new Set(urls));
        }
        """
    )


async def try_download_pdf_by_url(
    context: Any, pdf_url: str, destination: Path, timeout_ms: int
) -> bool:
    try:
        response = await context.request.get(pdf_url, timeout=timeout_ms)
    except PlaywrightError:
        return False
    if not response.ok:
        return False
    content_type = normalize(response.headers.get("content-type")).casefold()
    if "pdf" not in content_type and ".pdf" not in pdf_url.casefold():
        return False
    body = await response.body()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(body)
    return True


async def try_click_print_download(
    detail_page: Page, destination: Path, timeout_ms: int
) -> bool:
    selectors = [
        "a:has(img[src*='print'])",
        "a:has(img[src*='printer'])",
        "a:has-text('Nyomtat')",
        "button:has-text('Nyomtat')",
        "img[src*='print']",
        "img[src*='printer']",
    ]
    for selector in selectors:
        locator = detail_page.locator(selector)
        count = await locator.count()
        for idx in range(min(count, 5)):
            candidate = locator.nth(idx)
            try:
                if not await candidate.is_visible():
                    continue
            except PlaywrightError:
                continue

            try:
                async with detail_page.expect_download(timeout=timeout_ms) as dl_info:
                    await candidate.click()
                download = await dl_info.value
                destination.parent.mkdir(parents=True, exist_ok=True)
                await download.save_as(str(destination))
                return True
            except PlaywrightTimeoutError:
                continue
            except PlaywrightError:
                continue
    return False


async def click_detail_on_results_row(page: Page, row_index: int) -> dict[str, Any]:
    return await page.evaluate(
        """
        (idx) => {
          const normalize = (s) => (s || '').replace(/\\s+/g, ' ').trim();
          const visibleTables = Array.from(document.querySelectorAll('table'))
            .filter((t) => t.offsetParent !== null);

          let best = null;
          let bestRows = [];
          for (const table of visibleTables) {
            const tbodyRows = Array.from(table.querySelectorAll('tbody tr'));
            const allRows = tbodyRows.length > 0 ? tbodyRows : Array.from(table.querySelectorAll('tr'));
            if (allRows.length > bestRows.length) {
              best = table;
              bestRows = allRows;
            }
          }

          if (!best || bestRows.length === 0) return { ok: false, reason: 'nincs_talalati_tabla' };
          const target = bestRows[idx - 1];
          if (!target) return { ok: false, reason: 'nincs_sor' };

          const candidates = Array.from(
            target.querySelectorAll("a, button, input[type='button'], input[type='submit'], [onclick]")
          );
          if (candidates.length === 0) return { ok: false, reason: 'nincs_kattinthato' };

          const score = (el) => {
            const txt = normalize(el.innerText || el.value || el.title).toLowerCase();
            const href = normalize(el.getAttribute('href')).toLowerCase();
            const onclick = normalize(el.getAttribute('onclick')).toLowerCase();
            let s = 0;
            if (txt.includes('adatlap')) s += 10;
            if (txt.includes('megtekint')) s += 8;
            if (href.includes('adatlap') || onclick.includes('adatlap')) s += 9;
            if (href.includes('print') || onclick.includes('print')) s += 5;
            if (href.includes('pdf') || onclick.includes('pdf')) s += 5;
            return s;
          };

          let chosen = candidates[0];
          let bestScore = score(chosen);
          for (const candidate of candidates.slice(1)) {
            const currentScore = score(candidate);
            if (currentScore > bestScore) {
              chosen = candidate;
              bestScore = currentScore;
            }
          }
          if (bestScore <= 0) {
            return { ok: false, reason: 'nincs_adatlap_elem' };
          }

          const beforeUrl = window.location.href;
          chosen.click();
          const afterUrl = window.location.href;
          return {
            ok: true,
            reason: 'clicked',
            navigated: beforeUrl !== afterUrl,
            after_url: afterUrl,
          };
        }
        """,
        row_index,
    )


async def is_probably_detail_page(detail_page: Page) -> bool:
    return bool(
        await detail_page.evaluate(
            """
            () => {
              const text = (document.body?.innerText || '').toLowerCase();
              const hasDetailMarkers =
                text.includes('adatlapja') ||
                text.includes('alapnyilvántartási adatok') ||
                text.includes('egészségügyi tevékenység során használt név');

              const hasMainSearchMarkers =
                text.includes('tisztelt ügyfelünk') ||
                text.includes('zárva, húzza el a csúszkát') ||
                text.includes('név típusa');

              const hasPrintVisual =
                !!document.querySelector("img[src*='print'], img[src*='printer']");

              if (hasDetailMarkers) return true;
              if (hasMainSearchMarkers && !hasDetailMarkers) return false;
              return hasPrintVisual;
            }
            """
        )
    )


async def open_detail_page_from_results_row(
    page: Page, row: dict[str, Any], timeout_ms: int
) -> tuple[Page | None, str]:
    row_index_raw = row.get("_row_index")
    try:
        row_index = max(1, int(row_index_raw))
    except (TypeError, ValueError):
        return None, "nincs_row_index"

    click_result: dict[str, Any] | None = None
    old_url = page.url
    try:
        async with page.context.expect_page(timeout=timeout_ms) as popup_info:
            click_result = await click_detail_on_results_row(page, row_index)
        popup = await popup_info.value
        await popup.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        return popup, "popup"
    except PlaywrightTimeoutError:
        if click_result is None:
            click_result = await click_detail_on_results_row(page, row_index)
        await page.wait_for_timeout(350)
        if page.url != old_url or bool(click_result.get("navigated")):
            await page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
            return page, "same_tab"
        return None, normalize(click_result.get("reason")) or "detail_click_timeout"


async def extract_pdf_from_open_detail_page(
    detail_page: Page,
    request_context: Any,
    destination: Path,
    timeout_ms: int,
    page_pdf_fallback: bool,
) -> tuple[bool, str]:
    if not await is_probably_detail_page(detail_page):
        return False, "nem_adatlap_oldal"

    current_url = normalize(detail_page.url)
    if current_url and await try_download_pdf_by_url(
        request_context, current_url, destination, timeout_ms
    ):
        return True, "detail_url_pdf"

    pdf_links = await discover_pdf_links(detail_page)
    for pdf_url in pdf_links:
        if await try_download_pdf_by_url(request_context, pdf_url, destination, timeout_ms):
            return True, "pdf_link"

    if await try_click_print_download(detail_page, destination, timeout_ms):
        return True, "print_download"

    if page_pdf_fallback:
        if not await is_probably_detail_page(detail_page):
            return False, "nem_adatlap_oldal"
        destination.parent.mkdir(parents=True, exist_ok=True)
        await detail_page.emulate_media(media="print")
        await detail_page.pdf(
            path=str(destination),
            format="A4",
            print_background=True,
            margin={"top": "8mm", "right": "8mm", "bottom": "8mm", "left": "8mm"},
        )
        return True, "page_pdf"

    return False, "nincs_pdf_mod"


async def download_pdf_for_row(
    page: Page,
    row: dict[str, Any],
    output_dir: Path,
    timeout_ms: int,
    page_pdf_fallback: bool,
) -> tuple[bool, str]:
    detail_url = detail_url_from_row(row, base_url=page.url)
    destination = build_pdf_path(output_dir, row, detail_url or "row_click_fallback")
    if destination.exists():
        return True, "letezik"

    own_page = False
    opened_same_tab = False
    detail_page: Page | None = None
    try:
        if detail_url:
            detail_page = await page.context.new_page()
            own_page = True
            response = await detail_page.goto(
                detail_url, wait_until="domcontentloaded", timeout=timeout_ms
            )
            if response:
                content_type = normalize(response.headers.get("content-type")).casefold()
                if "pdf" in content_type:
                    body = await response.body()
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(body)
                    return True, "kozvetlen_pdf"
        else:
            detail_page, open_method = await open_detail_page_from_results_row(
                page=page, row=row, timeout_ms=timeout_ms
            )
            if detail_page is None:
                return False, open_method
            opened_same_tab = open_method == "same_tab"
            own_page = not opened_same_tab

        return await extract_pdf_from_open_detail_page(
            detail_page=detail_page,
            request_context=page.context,
            destination=destination,
            timeout_ms=timeout_ms,
            page_pdf_fallback=page_pdf_fallback,
        )
    except PlaywrightError:
        return False, "playwright_hiba"
    finally:
        if detail_page is not None and own_page:
            await detail_page.close()
        if opened_same_tab:
            try:
                await page.go_back(wait_until="domcontentloaded", timeout=timeout_ms)
                await wait_for_results(page, timeout_ms=min(3000, timeout_ms))
            except PlaywrightError:
                pass


async def download_pdfs_for_rows(
    page: Page,
    rows: list[dict[str, Any]],
    output_dir: Path,
    timeout_ms: int,
    page_pdf_fallback: bool,
    max_per_query: int,
) -> tuple[int, int]:
    ok_count = 0
    fail_count = 0
    selected_rows = rows[:max_per_query] if max_per_query > 0 else rows

    for row in selected_rows:
        success, method = await download_pdf_for_row(
            page=page,
            row=row,
            output_dir=output_dir,
            timeout_ms=timeout_ms,
            page_pdf_fallback=page_pdf_fallback,
        )
        reg_id = first_row_value_by_markers(
            row, ["nyilvantartasi", "nyilvántartási", "pecsetszam", "pecsétszám"]
        )
        if success:
            ok_count += 1
            print(f"    [pdf:ok] {reg_id or '-'} ({method})")
        else:
            fail_count += 1
            print(f"    [pdf:fail] {reg_id or '-'} ({method})")
    return ok_count, fail_count


def save_outputs(rows: list[dict[str, Any]], json_path: Path, csv_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, indent=2)

    columns: list[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                columns.append(key)

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


async def run(args: argparse.Namespace) -> None:
    if async_playwright is None:
        raise RuntimeError(
            "A 'playwright' csomag nincs telepitve. Futtasd: "
            "'pip install -r requirements.txt' es utana 'playwright install chromium'."
        )

    if args.db_only:
        args.download_pdfs = False

    alphabet = list(dict.fromkeys(list(args.alphabet)))
    queue: deque[str] = deque(alphabet)
    visited_terms: set[str] = set()
    collected: dict[str, dict[str, Any]] = {}
    unresolved_overflows: list[dict[str, Any]] = []
    pdf_ok_total = 0
    pdf_fail_total = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=args.headless)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        await page.goto(args.url, wait_until="domcontentloaded")

        if args.record_type:
            await maybe_select_record_type(page, args.record_type)

        while queue:
            term = queue.popleft()
            if term in visited_terms:
                continue
            visited_terms.add(term)

            print(f"[search] '{term}'")
            await fill_name(page, term)
            await trigger_name_input_events(page)

            solved = await try_auto_slider(page, timeout_ms=args.slider_wait_ms)
            if not solved and not args.allow_manual_slider:
                print(f"[warn] Slider auto-mode sikertelen: '{term}'")
                continue
            if not solved and args.allow_manual_slider:
                for attempt in range(1, args.manual_slider_attempts + 1):
                    await wait_for_manual_slider(term)
                    solved = await wait_for_search_enabled(
                        page, timeout_ms=args.manual_slider_wait_ms
                    )
                    if solved:
                        break
                    print(
                        f"[warn] Slider utan is disabled a Kereses gomb ('{term}', probalkozas={attempt})."
                    )

            await click_search(page, allow_force_submit=args.force_submit)
            await wait_for_results(page, timeout_ms=args.results_wait_ms)
            batch = await extract_rows(page)

            rows = batch["rows"]
            row_count = int(batch["rowCount"])
            total_hits = batch["totalHits"]

            print(f"  -> rows={row_count}, total_hits={total_hits}")

            is_truncated = row_count >= args.split_threshold
            if total_hits is not None and int(total_hits) > row_count:
                is_truncated = True

            if is_truncated and len(term) < args.max_depth:
                for ch in alphabet:
                    queue.append(term + ch)
                await page.wait_for_timeout(args.query_delay_ms)
                continue

            if is_truncated:
                unresolved_overflows.append(
                    {"term": term, "row_count": row_count, "total_hits": total_hits}
                )

            new_unique_rows: list[dict[str, Any]] = []
            for row in rows:
                row["_query"] = term
                key = row_key(row)
                if key not in collected:
                    new_unique_rows.append(row)
                collected[key] = row

            if args.download_pdfs and new_unique_rows:
                ok_count, fail_count = await download_pdfs_for_rows(
                    page=page,
                    rows=new_unique_rows,
                    output_dir=Path(args.pdf_dir),
                    timeout_ms=args.pdf_timeout_ms,
                    page_pdf_fallback=args.pdf_fallback_page_pdf,
                    max_per_query=args.max_pdfs_per_query,
                )
                pdf_ok_total += ok_count
                pdf_fail_total += fail_count

            await page.wait_for_timeout(args.query_delay_ms)

        await context.close()
        await browser.close()

    rows = list(collected.values())
    save_outputs(rows, Path(args.output_json), Path(args.output_csv))

    print(f"\nKesz. Egyedi sorok: {len(rows)}")
    print(f"JSON: {args.output_json}")
    print(f"CSV : {args.output_csv}")
    print(f"Lekerdezesek szama: {len(visited_terms)}")
    if args.download_pdfs:
        print(f"PDF mappa: {args.pdf_dir}")
        print(f"PDF sikeres: {pdf_ok_total}")
        print(f"PDF sikertelen: {pdf_fail_total}")
    if unresolved_overflows:
        overflow_path = Path(args.overflow_json)
        overflow_path.parent.mkdir(parents=True, exist_ok=True)
        with overflow_path.open("w", encoding="utf-8") as f:
            json.dump(unresolved_overflows, f, ensure_ascii=False, indent=2)
        print(f"Nem feloldott 100-as csoportok: {len(unresolved_overflows)}")
        print(f"Overflow lista: {args.overflow_json}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orvos adat scrape a kereso.enkk.hu oldalrol Playwrighttal."
    )
    parser.add_argument("--url", default="https://kereso.enkk.hu/")
    parser.add_argument("--output-json", default="data/orvosok.json")
    parser.add_argument("--output-csv", default="data/orvosok.csv")
    parser.add_argument("--overflow-json", default="data/overflow_terms.json")
    parser.add_argument(
        "--alphabet",
        default="aábcdeéfghiíjklmnoóöőpqrstuúüűvwxyz",
        help="Karakterkeszlet a prefix alapu bontashoz.",
    )
    parser.add_argument("--max-depth", type=int, default=3)
    parser.add_argument("--split-threshold", type=int, default=100)
    parser.add_argument("--query-delay-ms", type=int, default=900)
    parser.add_argument("--results-wait-ms", type=int, default=5000)
    parser.add_argument("--slider-wait-ms", type=int, default=350)
    parser.add_argument("--manual-slider-attempts", type=int, default=3)
    parser.add_argument("--manual-slider-wait-ms", type=int, default=3000)
    parser.add_argument(
        "--record-type",
        default="Orvos/fogorvos",
        help="Alapnyilvantartas tipus opcio reszlete.",
    )
    parser.add_argument(
        "--allow-manual-slider",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Engedelyezett-e a kezi slider fallback.",
    )
    parser.add_argument(
        "--force-submit",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Ha disabled marad a gomb, JS form submit fallback hasznalata.",
    )
    parser.add_argument(
        "--download-pdfs",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="A talalati sorokhoz tartozo adatlap PDF-ek mentese.",
    )
    parser.add_argument(
        "--db-only",
        action="store_true",
        help="Csak JSON/CSV kimenet, PDF letoltes nelkul.",
    )
    parser.add_argument("--pdf-dir", default="data/pdfs")
    parser.add_argument("--pdf-timeout-ms", type=int, default=15000)
    parser.add_argument(
        "--pdf-fallback-page-pdf",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Ha nincs direkt PDF, oldalbol generalt PDF fallback hasznalata.",
    )
    parser.add_argument(
        "--max-pdfs-per-query",
        type=int,
        default=0,
        help="0 = nincs limit, kulonben ennyi PDF mentese lekerdezesenkent.",
    )
    parser.add_argument("--headless", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        print("\nMegszakitva.")
