#!/usr/bin/env python
import argparse
import asyncio
import contextlib
import functools
import random
import sys
from typing import Sequence, TextIO
from urllib.parse import urljoin

from aiolimiter import AsyncLimiter
from playwright.async_api import Browser, Playwright, async_playwright

CSI = "\033["
RESET = f"{CSI}0m"
BLACK = f"{CSI}30m"
RED = f"{CSI}31m"
GREEN = f"{CSI}32m"
YELLOW = f"{CSI}33m"
BLUE = f"{CSI}34m"
MAGENTA = f"{CSI}35m"
CYAN = f"{CSI}36m"
GREY = f"{CSI}37m"

print_stderr = functools.partial(print, file=sys.stderr)

parser = argparse.ArgumentParser(
    description="Parses company websites from clutch.co.",
    epilog="Hint: you need to run browser with flag --remote-debugging-port=9222.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "-b",
    "--browser-url",
    help="browser url",
    default="http://localhost:9222",
)
parser.add_argument(
    "-o",
    "--output",
    help="output file",
    default="-",
    type=argparse.FileType("w"),
)
parser.add_argument(
    "-c",
    "--concurrency",
    help="number of concurrent jobs",
    default=4,
    type=int,
)
parser.add_argument(
    "-rl",
    "--rate-limit",
    "--limit",
    help="requests per second",
    default=4,
    type=int,
)
parser.add_argument(
    "--randomize",
    "--rand",
    help="randomize profile links",
    default=False,
    action="store_true",
)


async def main(argv: Sequence[str] | None = None) -> None:
    args = parser.parse_args(argv)
    playwright: Playwright
    async with async_playwright() as playwright:
        browser: Browser = await playwright.chromium.connect_over_cdp(args.browser_url)

        profile_links = await get_profile_links(browser)

        print_stderr(f"{GREEN}total profile links: {len(profile_links)}{RESET}")

        if args.randomize:
            print_stderr(f"{CYAN}randomize profile links{RESET}")
            random.shuffle(profile_links)

        # Добавляем их в очередь
        q = asyncio.Queue()

        for x in profile_links:
            q.put_nowait(x)

        limiter = AsyncLimiter(max_rate=args.rate_limit, time_period=1)

        await asyncio.sleep(1.0)

        with contextlib.suppress(asyncio.CancelledError):
            async with asyncio.TaskGroup() as g:
                for _ in range(args.concurrency):
                    g.create_task(worker(browser, q, args.output, limiter))

                await q.join()

                for _ in range(args.concurrency):
                    q.put_nowait(None)

        await browser.close()

    print_stderr(f"{YELLOW}all tasks finished!{RESET}")


async def get_profile_links(browser: Browser) -> list[str]:
    context = await browser.new_context()
    page = await context.new_page()
    # Получаем ссылки на все профили компаний
    await page.goto("https://clutch.co/sitemap.xml", wait_until="load")

    try:
        return await page.evaluate(r"""
            async function() {
                async function parseProfileLinks(url) {
                    let response = await fetch(url)
                    let text = await response.text()
                    return [...text.matchAll(/(?<=<loc>)[^<>]+/g)].flat()
                }
                let tasks = [...document.querySelectorAll('loc')]
                    .map(q => q.innerHTML)
                    .filter(q => /\/sitemap-profile-\d+\.xml$/.test(q))
                    .map(parseProfileLinks)
                let results = await Promise.all(tasks)
                return results.flat()
            }
            """)
    finally:
        await context.close()


async def worker(
    browser: Browser,
    q: asyncio.Queue,
    output: TextIO,
    limiter: AsyncLimiter,
) -> None:
    context = await browser.new_context()
    page = await context.new_page()

    await page.set_viewport_size({"width": 1024, "height": 768})

    while True:
        link = await q.get()

        try:
            if link is None:
                break

            async with limiter:
                print_stderr(f"{CYAN}start loading: {link}{RESET}")
                response = await page.goto(link, wait_until="load")

            print_stderr(f"{CYAN}page loaded: {response.url}{RESET}")

            # await page.wait_for_selector(".profile-header__title", timeout=5000)

            if not (
                website_link := (
                    await page.evaluate(
                        "document.querySelector('.website-link__item')?.href"
                    )
                )
            ):
                continue

            website_link = urljoin(website_link, "/")
            print_stderr(f"{GREEN}found website: {website_link}{RESET}")
            output.write(f"{website_link}\n")
            output.flush()
        except Exception as ex:
            # иногда начинает 403 возвращать, просто подождем
            print_stderr(f"{RED}error: {ex}{RESET}")
        finally:
            q.task_done()
            print_stderr(f"{CYAN}queue size: {q.qsize()}{RESET}")

    await context.close()


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(main())
